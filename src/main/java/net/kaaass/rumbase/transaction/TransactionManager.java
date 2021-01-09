package net.kaaass.rumbase.transaction;

/**
 * 事务管理器
 *
 * <p>
 * 管理事务的状态
 * </p>
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
     * 改变事务日志中的事务状态
     *
     * @param xid    事务id
     * @param status 新事务状态
     */
    void changeTransactionStatus(int xid, TransactionStatus status);

}
