package net.kaaass.rumbase.transaction.lock;

import net.kaaass.rumbase.transaction.exception.DeadlockException;

/**
 * 锁表
 * <p>
 * 记录锁的信息
 *
 * @author criki
 */
public interface LockTable {
    /**
     * 添加共享锁
     *
     * @param xid       事务id
     * @param uuid      记录id
     * @param tableName 表名
     * @throws DeadlockException 发生死锁异常
     */
    void addSharedLock(int xid, long uuid, String tableName) throws DeadlockException;

    /**
     * 添加排他锁
     *
     * @param xid       事务id
     * @param uuid      记录id
     * @param tableName 表名
     * @throws DeadlockException 发生死锁异常
     */
    void addExclusiveLock(int xid, long uuid, String tableName) throws DeadlockException;

    /**
     * 释放事务的锁
     *
     * @param xid 事务id
     */
    void release(int xid);


}
