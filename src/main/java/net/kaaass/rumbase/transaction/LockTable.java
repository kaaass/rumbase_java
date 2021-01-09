package net.kaaass.rumbase.transaction;

/**
 * 锁表
 * <p>
 * 记录锁的信息
 *
 * @author criki
 */
public interface LockTable {
    /**
     * 添加共享锁 FIXME：锁表也全局，需要表字段（字符串）。但是因为表字段重复多，但是不同表字段名重复几乎没有可能，所以存储的时候应该优先用uuid判断
     *
     * @param xid  事务id
     * @param uuid 记录id
     */
    void addSharedLock(int xid, long uuid);

    /**
     * 添加排他锁 FIXME：锁表也全局，需要表字段（字符串）。但是因为表字段重复多，但是不同表字段名重复几乎没有可能，所以存储的时候应该优先用uuid判断
     *
     * @param xid  事务id
     * @param uuid 记录id
     */
    void addExclusiveLock(int xid, long uuid);

    /**
     * 释放事务的锁
     *
     * @param xid 事务id
     */
    void release(int xid);


}
