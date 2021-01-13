package net.kaaass.rumbase.transaction;

/**
 * 事务管理器
 * <p>
 * 管理事务的状态
 *
 * @author criki
 */
public interface TransactionManager {

    /**
     * 创建新事务
     *
     * @param isolation 事务隔离度
     * @return 创建的事物对象
     */
    TransactionContext createTransactionContext(TransactionIsolation isolation);

    /**
     * 根据事务id获取事务上下文
     *
     * @param xid 事务id
     * @return 事务id为xid的事务上下文
     */
    TransactionContext getTransactionContext(int xid);

    /**
     * 改变事务日志中的事务状态
     *
     * @param xid    事务id
     * @param status 新事务状态
     */
    void changeTransactionStatus(int xid, TransactionStatus status);

}
