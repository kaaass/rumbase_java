package net.kaaass.rumbase.server;

import net.kaaass.rumbase.transaction.TransactionContext;

/**
 * 存储、管理会话中的数据库状态
 * @author kaaass
 */
public class Session {

    private TransactionContext currentContext = null;

    /**
     * 执行SQL语句并输出相关结果
     */
    public void executeSql(String sql) {
        // TODO
    }
}
