package net.kaaass.rumbase.transaction;

/**
 * 锁表
 *
 * <p>
 * 记录锁的信息
 * </p>
 *
 * @author criki
 */
public interface LockTable {
    /**
     * 添加共享锁
     *
     * @param xid  事务id
     * @param uuid 记录id
     */
    void addSharedLock(int xid, long uuid);

    /**
     * 添加排他锁
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
