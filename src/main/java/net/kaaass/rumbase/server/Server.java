package net.kaaass.rumbase.server;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.index.exception.IndexAlreadyExistException;
import net.kaaass.rumbase.page.PageManager;
import net.kaaass.rumbase.page.exception.FileException;
import net.kaaass.rumbase.query.exception.ArgumentException;
import net.kaaass.rumbase.record.exception.RecordNotFoundException;
import net.kaaass.rumbase.table.TableManager;
import net.kaaass.rumbase.table.exception.TableConflictException;
import net.kaaass.rumbase.table.exception.TableExistenceException;
import net.kaaass.rumbase.transaction.TransactionManager;
import net.kaaass.rumbase.transaction.TransactionManagerImpl;

import java.io.File;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 管理服务器运行过程中总体的运行状态，使用单例模式
 *
 * @author kaaass
 */
@Slf4j
public class Server {

    @Getter
    private TransactionManager transactionManager = null;

    private ExecutorService threadPool = null;

    @Getter
    @Setter
    private boolean acceptNewConnection = true;

    @Getter
    private TableManager tableManager = null;

    private final AtomicLong sessionCounter = new AtomicLong(0);

    @Getter
    private final ConcurrentSkipListSet<Session> activeSession = new ConcurrentSkipListSet<>();

    /**
     * 进行服务器开始前的准备工作
     */
    public void prepare() {
        // 准备文件夹
        var tableFolder = new File("data/table/a");
        assert tableFolder.exists() || tableFolder.mkdirs();
        var indexFolder = new File("data/index/a");
        assert indexFolder.exists() || indexFolder.mkdirs();
        // 初始化事务管理器
        log.info("初始化事务管理器...");
        try {
            transactionManager = new TransactionManagerImpl();
        } catch (IOException | FileException e) {
            log.error("初始化事务管理器失败", e);
            System.exit(1);
        }
        // 初始化表管理器
        log.info("初始化表管理器...");
        // TODO 先恢复metadata
        try {
            tableManager = new TableManager();
        } catch (TableExistenceException | TableConflictException | RecordNotFoundException | ArgumentException | IndexAlreadyExistException e) {
            log.error("初始化表管理器失败", e);
            System.exit(1);
        }
        // 初始化线程池
        log.info("初始化线程池...");
        var namedThreadFactory = Executors.defaultThreadFactory();
        threadPool = new ThreadPoolExecutor(5, 200,
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(1024), namedThreadFactory, new ThreadPoolExecutor.AbortPolicy());
    }

    /**
     * 运行服务器，监听客户端消息
     */
    public void run() {
        // 监听端口
        try (ServerSocket server = new ServerSocket(8889)) {
            log.info("开始监听 {}", server.getLocalSocketAddress());

            server.setSoTimeout(1000);
            while (acceptNewConnection) {
                try {
                    var connection = server.accept();
                    long sessionId = sessionCounter.incrementAndGet();
                    var session = new Session(sessionId, connection, this);
                    threadPool.submit(session);
                    log.info("创建会话 {}", sessionId);
                } catch (SocketTimeoutException ignore) {
                }
            }
        } catch (IOException e) {
            log.error("服务器启动失败", e);
        }
    }

    /**
     * 服务器关闭
     */
    public void shutdown() {
        // 关闭线程池
        log.info("正在关闭线程池...");
        if (threadPool != null) {
            threadPool.shutdown();
        }
        // 关闭所有活动会话
        log.info("正在关闭所有活动会话...");
        for (var session : activeSession) {
            session.say("服务器正在关闭...\n");
            session.onClose();
        }
        activeSession.clear();
        // 释放文件、写回文件
        log.info("正在写回文件...");
        PageManager.flush();
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
