package net.kaaass.rumbase.server;

import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.page.exception.FileException;
import net.kaaass.rumbase.transaction.TransactionManager;
import net.kaaass.rumbase.transaction.TransactionManagerImpl;

import java.io.IOException;

/**
 * 管理服务器运行过程中总体的运行状态，使用单例模式
 * @author kaaass
 */
@Slf4j
public class Server {

    private TransactionManager transactionManager = null;

    /**
     * 进行服务器开始前的准备工作
     */
    public void prepare() {
        // 初始化事务管理器
        try {
            transactionManager = new TransactionManagerImpl();
        } catch (IOException | FileException e) {
            log.error("初始化事务管理器失败", e);
            System.exit(1);
        }
        // TODO
    }

    /**
     * 运行服务器，监听客户端消息
     */
    public void run() {
        // TODO
    }

    private static final Server INSTANCE = new Server();

    private Server() {
    }

    /**
     * 获得Server单例
     */
    public static Server getInstance() {
        return INSTANCE;
    }
}
