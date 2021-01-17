package net.kaaass.rumbase.server;

import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.exception.RumbaseException;
import net.kaaass.rumbase.exception.RumbaseRuntimeException;
import net.kaaass.rumbase.index.exception.IndexAlreadyExistException;
import net.kaaass.rumbase.page.PageManager;
import net.kaaass.rumbase.parse.ISqlStatement;
import net.kaaass.rumbase.parse.ISqlStatementVisitor;
import net.kaaass.rumbase.parse.SqlParser;
import net.kaaass.rumbase.parse.exception.SqlSyntaxException;
import net.kaaass.rumbase.parse.stmt.*;
import net.kaaass.rumbase.query.*;
import net.kaaass.rumbase.query.exception.ArgumentException;
import net.kaaass.rumbase.record.exception.RecordNotFoundException;
import net.kaaass.rumbase.table.exception.TableConflictException;
import net.kaaass.rumbase.table.exception.TableExistenceException;
import net.kaaass.rumbase.transaction.TransactionContext;
import net.kaaass.rumbase.transaction.TransactionIsolation;

import java.io.*;
import java.net.Socket;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.stream.Collectors;

/**
 * 存储、管理会话中的数据库状态
 *
 * @author kaaass
 */
@Slf4j
@RequiredArgsConstructor
@EqualsAndHashCode(of = "sessionId")
public class Session implements Runnable, Comparable<Session>, ISqlStatementVisitor<Boolean> {

    private TransactionContext currentContext = null;

    private final long sessionId;

    private final Socket connection;

    private final Server server;

    private Scanner scanner = null;

    private Writer writer = null;

    private boolean autoCommit = false;

    /**
     * 执行SQL语句并输出相关结果
     * @return 是否退出
     */
    public boolean executeSql(String sql) {
        // 解析SQL语句
        ISqlStatement stmt;
        try {
            stmt = SqlParser.parseStatement(sql);
        } catch (SqlSyntaxException e) {
            log.debug("会话 {} 解析SQL语句失败，输入： {}", sessionId, sql, e);
            say(e);
            return false;
        } catch (Exception e) {
            log.warn("会话 {} 解析SQL语句出现异常错误，输入：{}", sessionId, sql, e);
            say("解析异常，请检查服务器日志\n");
            return false;
        }
        log.debug("会话 {} 解析SQL语句: {}", sessionId, stmt);
        // 执行SQL
        try {
            return stmt.accept(this);
            // TODO 自动回滚事务、SQL日志
        } catch (RumbaseRuntimeException e) {
            log.info("会话 {} 发生错误", sessionId, e);
            say(e);
            return false;
        } catch (Exception e) {
            log.warn("会话 {} 运行SQL语句出现未知异常，输入：{}", sessionId, sql, e);
            say("发生未知异常，请检查服务器日志\n");
            return false;
        }
    }

    @Override
    public void run() {
        // 加载
        onInit();
        // 加载成功，加入活跃会话
        server.getActiveSession().add(this);
        // 进入REPL模式
        var exit = false;
        while (!exit && !connection.isClosed()) {
            // 打印命令提示符
            say("\n> ");
            try {
                writer.flush();
            } catch (IOException ignore) {
            }
            try {
                // 等待输入
                String input = scanner.nextLine();
                if (input.isBlank()) {
                    continue;
                }
                // 执行
                exit = executeSql(input);
            } catch (NoSuchElementException e) {
                log.info("会话 {} 被强制关闭", sessionId, e);
                break;
            } catch (Exception e) {
                log.info("会话 {} 命令执行错误", sessionId, e);
                say("未知错误，请检查服务端日志\n");
            }
        }
        // 退出
        onClose();
    }

    /**
     * 当会话开始加载
     */
    public void onInit() {
        // 准备读写
        try {
            scanner = new Scanner(new BufferedInputStream(connection.getInputStream()));
            writer = new OutputStreamWriter(new BufferedOutputStream(connection.getOutputStream()));
        } catch (IOException e) {
            log.info("无法处理IO，会话建立失败", e);
        }
        // 打印欢迎信息
        say("Welcome to Rumbase DMBS\n\n");
    }

    /**
     * 当会话被关闭
     */
    public void onClose() {
        log.info("正在关闭会话 {} ...", sessionId);
        // 提交活动事务
        if (currentContext != null) {
            say("正在提交当前事务...\n");
            try {
                currentContext.commit();
            } catch (RumbaseRuntimeException e) {
                log.warn("退出会话 {} 时提交事务失败", sessionId, e);
                say(e);
            }
        }
        // 删除活跃会话
        server.getActiveSession().remove(this);
        // 退出成功
        say("Bye\n");
        try {
            writer.close();
            scanner.close();
        } catch (IOException ignore) {
        }
    }

    public void say(CharSequence chars) {
        try {
            writer.append(chars);
        } catch (IOException e) {
            log.info("信息发送失败：{}，会话：{}", chars, sessionId, e);
        }
    }

    private void say(RumbaseException e) {
        say(e.getMessage() + "\n");
    }

    private void say(RumbaseRuntimeException e) {
        say(e.getMessage() + "\n");
    }

    @Override
    public int compareTo(Session o) {
        return Long.compare(sessionId, o.sessionId);
    }

    @Override
    public Boolean visit(SelectStatement statement) {
        checkAutoCommitBefore();
        // 执行语句
        try {
            var executor = new SelectExecutor(statement,
                    server.getTableManager(),
                    currentContext);
            executor.execute();
            saySelectResult(executor, statement.getFromTable());
        } catch (TableExistenceException | IndexAlreadyExistException | ArgumentException | TableConflictException | RecordNotFoundException e) {
            log.debug("会话 {} 执行语句异常", sessionId, e);
            say(e);
        } catch (Exception e) {
            // 发生任何错误都回滚
            checkAutoCommitAfter(true);
            throw e;
        }
        checkAutoCommitAfter(false);
        return false;
    }

    /**
     * 以表格格式输出选择语句的结果
     * @param executor 结果集
     */
    private void saySelectResult(SelectExecutor executor, String defaultTable) {
        var columns =
                executor.getResultTable().stream()
                    .map(column -> column.getTableName().equals(defaultTable) ?
                            column.getFieldName() :
                            column.getTableName() + "." + column.getFieldName())
                    .collect(Collectors.toList());
        var rows = executor.getResultData();
        log.debug("查询结果 {}, {}", columns, rows);
        // 格式化为表格
        int[] maxLengths = new int[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            maxLengths[i] = columns.get(i).length();
        }
        for (var row : rows) {
            for (int i = 0; i < row.size(); i++) {
                row.set(i, row.get(i) == null ? "NULL" : row.get(i).toString());
                maxLengths[i] = Math.max(maxLengths[i], ((String) row.get(i)).length());
            }
        }
        // 生成行格式
        StringBuilder formatBuilder = new StringBuilder("|");
        for (int maxLength : maxLengths) {
            formatBuilder.append("%-").append(maxLength + 2).append("s |");
        }
        String format = formatBuilder.toString();
        // 输出行首
        var result = new StringBuilder();
        result.append(String.format(format, columns.toArray(new Object[0]))).append("\n");
        for (int i = 0; i < columns.size(); i++) {
            result.append(i == 0 ? '|' : '+');
            result.append("-".repeat(Math.max(0, maxLengths[i] + 3)));
        }
        result.append("|\n");
        // 输出内容
        for (var row : rows) {
            result.append(String.format(format, row.toArray(new Object[0]))).append("\n");
        }
        say(result);
        say("共 " + rows.size() + " 条记录\n");
    }

    @Override
    public Boolean visit(InsertStatement statement) {
        checkAutoCommitBefore();
        // 执行语句
        try {
            var executor = new InsertExecutor(statement,
                    server.getTableManager(),
                    currentContext);
            executor.execute();
            say("语句执行成功\n");
        } catch (TableExistenceException | ArgumentException | TableConflictException e) {
            log.debug("会话 {} 执行语句异常", sessionId, e);
            say(e);
        } catch (Exception e) {
            // 发生任何错误都回滚
            checkAutoCommitAfter(true);
            throw e;
        }
        checkAutoCommitAfter(false);
        return false;
    }

    @Override
    public Boolean visit(UpdateStatement statement) {
        checkAutoCommitBefore();
        // 执行语句
        try {
            var executor = new UpdateExecutor(statement,
                    server.getTableManager(),
                    currentContext);
            executor.execute();
            say("语句执行成功\n");
        } catch (TableExistenceException | IndexAlreadyExistException | ArgumentException | TableConflictException | RecordNotFoundException e) {
            log.debug("会话 {} 执行语句异常", sessionId, e);
            say(e);
        } catch (Exception e) {
            // 发生任何错误都回滚
            checkAutoCommitAfter(true);
            throw e;
        }
        checkAutoCommitAfter(false);
        return false;
    }

    @Override
    public Boolean visit(DeleteStatement statement) {
        checkAutoCommitBefore();
        // 执行语句
        try {
            var executor = new DeleteExecutor(statement,
                    server.getTableManager(),
                    currentContext);
            executor.execute();
            say("语句执行成功\n");
        } catch (TableExistenceException | IndexAlreadyExistException | ArgumentException | TableConflictException | RecordNotFoundException e) {
            log.debug("会话 {} 执行语句异常", sessionId, e);
            say(e);
        } catch (Exception e) {
            // 发生任何错误都回滚
            checkAutoCommitAfter(true);
            throw e;
        }
        checkAutoCommitAfter(false);
        return false;
    }

    @Override
    public Boolean visit(CreateIndexStatement statement) {
        checkAutoCommitBefore();
        // 执行语句
        try {
            var executor = new CreateIndexExecutor(statement,
                    server.getTableManager(), currentContext);
            executor.execute();
            say("成功创建索引\n");
        } catch (TableExistenceException | IndexAlreadyExistException e) {
            log.debug("会话 {} 执行语句异常", sessionId, e);
            say(e);
        } catch (Exception e) {
            // 发生任何错误都回滚
            checkAutoCommitAfter(true);
            throw e;
        }
        checkAutoCommitAfter(false);
        return false;
    }

    @Override
    public Boolean visit(CreateTableStatement statement) {
        checkAutoCommitBefore();
        // 执行语句
        try {
            var executor = new CreateTableExecutor(statement,
                    server.getTableManager(),
                    currentContext);
            executor.execute();
            say("成功创建表\n");
        } catch (ArgumentException | TableConflictException | TableExistenceException | RecordNotFoundException e) {
            log.debug("会话 {} 执行语句异常", sessionId, e);
            say(e);
        } catch (Exception e) {
            // 发生任何错误都回滚
            checkAutoCommitAfter(true);
            throw e;
        }
        checkAutoCommitAfter(false);
        return false;
    }

    /**
     * 执行前尝试自动提交
     */
    private void checkAutoCommitBefore() {
        assert !autoCommit;
        if (currentContext == null) {
            // 自动创建事务
            // TODO 默认使用可重复读隔离度，应该从配置读取
            currentContext = server.getTransactionManager().createTransactionContext(TransactionIsolation.REPEATABLE_READ);
            // 设置自动提交
            autoCommit = true;
        }
    }

    /**
     * 执行后尝试自动提交
     * @param rollback 是否需要回滚
     */
    private void checkAutoCommitAfter(boolean rollback) {
        if (autoCommit) {
            try {
                assert currentContext != null;
                if (rollback) {
                    currentContext.rollback();
                } else {
                    currentContext.commit();
                }
                currentContext = null;
            } finally {
                // 完成自动提交
                autoCommit = false;
            }
        }
    }

    @Override
    public Boolean visit(StartTransactionStatement statement) {
        if (currentContext != null) {
            say("请先提交当前事务！\n");
            return false;
        }
        // 创建事务
        // TODO 默认使用可重复读隔离度，应该从配置读取
        currentContext = server.getTransactionManager().createTransactionContext(TransactionIsolation.REPEATABLE_READ);
        say("成功创建事务" + currentContext.getXid() + "\n");
        return false;
    }

    @Override
    public Boolean visit(CommitStatement statement) {
        if (currentContext == null) {
            say("当前会话内无事务\n");
            return false;
        }
        // 提交事务
        currentContext.commit();
        say("成功提交事务" + currentContext.getXid() + "\n");
        currentContext = null;
        return false;
    }

    @Override
    public Boolean visit(RollbackStatement statement) {
        if (currentContext == null) {
            say("当前会话内无事务\n");
            return false;
        }
        // 回滚事务
        currentContext.rollback();
        say("成功回滚事务" + currentContext.getXid() + "\n");
        currentContext = null;
        return false;
    }

    @Override
    public Boolean visit(ExitStatement statement) {
        say("正在关闭会话...\n");
        return true;
    }

    @Override
    public Boolean visit(ShutdownStatement statement) {
        say("正在关闭服务器...\n");
        System.exit(0);
        return true;
    }

    @Override
    public Boolean visit(FlushStatement statement) {
        PageManager.flush();
        say("已刷新缓冲\n");
        return false;
    }

    @Override
    public Boolean visit(ExecStatement statement) {
        // 读入文件
        List<String> lines;
        try {
            lines = Files.readAllLines(Paths.get(statement.getFilepath()));
        } catch (IOException e) {
            say("文件读取失败，请检查文件是否存在\n");
            return false;
        }
        // 逐行解析
        for (var line : lines) {
            if (line.isBlank() || line.startsWith("#")) {
                continue;
            }
            this.executeSql(line);
        }
        return false;
    }
}
