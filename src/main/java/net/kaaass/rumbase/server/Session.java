package net.kaaass.rumbase.server;

import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.exception.RumbaseException;
import net.kaaass.rumbase.parse.ISqlStatement;
import net.kaaass.rumbase.parse.ISqlStatementVisitor;
import net.kaaass.rumbase.parse.SqlParser;
import net.kaaass.rumbase.parse.exception.SqlSyntaxException;
import net.kaaass.rumbase.parse.stmt.*;
import net.kaaass.rumbase.transaction.TransactionContext;

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
        say("Parsed: " + stmt.toString() + "\n");
        // 执行SQL
        return stmt.accept(this);
    }

    @Override
    public void run() {
        // 准备读写
        try {
            scanner = new Scanner(new BufferedInputStream(connection.getInputStream()));
            writer = new OutputStreamWriter(new BufferedOutputStream(connection.getOutputStream()));
        } catch (IOException e) {
            log.info("无法处理IO，会话建立失败", e);
        }
        // 打印欢迎信息
        say("Welcome to Rumbase DMBS\n\n");
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
     * 当会话被关闭
     */
    public void onClose() {
        log.info("关闭会话 {} ...", sessionId);
        // TODO
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
}
