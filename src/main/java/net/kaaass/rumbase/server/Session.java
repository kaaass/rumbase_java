package net.kaaass.rumbase.server;

import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.exception.RumbaseException;
import net.kaaass.rumbase.exception.RumbaseRuntimeException;
import net.kaaass.rumbase.parse.ISqlStatement;
import net.kaaass.rumbase.parse.ISqlStatementVisitor;
import net.kaaass.rumbase.parse.SqlParser;
import net.kaaass.rumbase.parse.exception.SqlSyntaxException;
import net.kaaass.rumbase.parse.stmt.*;
import net.kaaass.rumbase.transaction.TransactionContext;
import net.kaaass.rumbase.transaction.TransactionIsolation;

import java.io.*;
import java.net.Socket;
import java.util.NoSuchElementException;
import java.util.Scanner;

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
            say("解析异常，请检查服务器日志");
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
            say("发生未知异常，请检查服务器日志");
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
        // TODO
        return false;
    }

    @Override
    public Boolean visit(InsertStatement statement) {
        // TODO
        return false;
    }

    @Override
    public Boolean visit(UpdateStatement statement) {
        // TODO
        return false;
    }

    @Override
    public Boolean visit(DeleteStatement statement) {
        // TODO
        return false;
    }

    @Override
    public Boolean visit(CreateIndexStatement statement) {
        // TODO
        return false;
    }

    @Override
    public Boolean visit(CreateTableStatement statement) {
        // TODO
        return false;
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
}
