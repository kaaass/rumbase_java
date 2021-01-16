package net.kaaass.rumbase;

import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.server.Server;

/**
 * 入口类
 *
 * @author kaaass
 */
@Slf4j
public class Main {

    public static void main(String[] args) {
        log.info("Rumbase DBMS");
        // 启动
        log.info("Start preparing...");
        Server.getInstance().prepare();
        // 运行
        log.info("Starting server...");
        Server.getInstance().run();
        // 注册程序退出事件
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Shutdown...");
            Server.getInstance().shutdown();
        }));
    }
}
