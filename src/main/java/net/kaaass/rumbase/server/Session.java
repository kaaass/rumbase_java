package net.kaaass.rumbase.server;

import lombok.EqualsAndHashCode;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.transaction.TransactionContext;

import java.io.*;
import java.net.Socket;
import java.util.NoSuchElementException;
import java.util.Scanner;
import java.util.concurrent.Callable;

/**
 * 存储、管理会话中的数据库状态
 *
 * @author kaaass
 */
@Slf4j
@RequiredArgsConstructor
@EqualsAndHashCode(of = "sessionId")
public class Session implements Callable<Void>, Comparable<Session> {

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
        say("Echo: " + sql + "\n");
        // TODO
        return false;
    }

    @Override
    public Void call() {
        // 准备读写
        try {
            scanner = new Scanner(new BufferedInputStream(connection.getInputStream()));
            writer = new OutputStreamWriter(new BufferedOutputStream(connection.getOutputStream()));
        } catch (IOException e) {
            log.info("无法处理IO，会话建立失败", e);
        }
        // 打印欢迎信息
        say("Welcome to Rumbase DMBS\n\n\n");
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
                // 执行
                exit = executeSql(input);
            } catch (NoSuchElementException e) {
                log.info("会话 {} 被强制关闭", sessionId, e);
                break;
            } catch (Exception e) {
                log.info("会话 {} 命令执行错误", sessionId, e);
            }
        }
        // 退出
        onClose();
        return null;
    }

    /**
     * 当会话被关闭
     */
    public void onClose() {
        log.info("关闭会话 {} ...", sessionId);
        // TODO
    }

    private void say(CharSequence chars) {
        try {
            writer.append(chars);
        } catch (IOException e) {
            log.info("信息发送失败：{}，会话：{}", chars, sessionId, e);
        }
    }

    @Override
    public int compareTo(Session o) {
        return Long.compare(sessionId, o.sessionId);
    }
}
