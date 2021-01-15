package net.kaaass.rumbase.transaction;

import net.kaaass.rumbase.transaction.exception.DeadlockException;

import java.util.List;

/**
 * 事务上下文
 * <p>
 * 存储事务状态，对事务进行操作
 *
 * @author criki
 */
public interface TransactionContext {


    /**
     * 返回空事务上下文。空事务或超级事务XID为0，不受事务限制，也不具备ACID性质。
     *
     * @return 空事务上下文
     */
    static TransactionContext empty() {
        return new TransactionContextImpl();
    }

    /**
     * 获取当前事务快照信息
     *
     * @return 事务快照
     */
    List<Integer> getSnapshot();

    /**
     * 获取事务隔离度
     *
     * @return 事务隔离度
     */
    TransactionIsolation getIsolation();

    /**
     * 获取事务状态
     *
     * @return 事务状态
     */
    TransactionStatus getStatus();

    /**
     * 获取事务管理器
     *
     * @return 事务管理器
     */
    TransactionManager getManager();

    /**
     * 获取事务id
     *
     * @return 事务id
     */
    int getXid();

    /**
     * 事务开始
     */
    void start();

    /**
     * 事务提交
     */
    void commit();

    /**
     * 事务撤销
     */
    void rollback();

    /**
     * 对记录加共享锁
     *
     * @param uuid      记录id
     * @param tableName 表字段
     * @throws DeadlockException 发生死锁异常
     */
    void sharedLock(long uuid, String tableName) throws DeadlockException;

    /**
     * 对记录加排他锁
     *
     * @param uuid      记录id
     * @param tableName 表字段
     * @throws DeadlockException 发生死锁异常
     */
    void exclusiveLock(long uuid, String tableName) throws DeadlockException;
}
