package net.kaaass.rumbase.transaction.lock;

import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 事务项
 * <p>
 * 锁表中记录事务加锁信息的单元
 *
 * @author criki
 */
public class TxItem {
    /**
     * 判断是否被分配锁
     */
    public boolean granted;

    /**
     * 事务id
     */
    public int xid;

    /**
     * 加锁类型
     */
    public LockMode mode;

    /**
     * 事务项的互斥锁
     */
    public Lock lock = new ReentrantLock();

    /**
     * 分配锁条件量
     */
    public Condition grantLock = lock.newCondition();

    /**
     * 事务项
     *
     * @param granted 是否已分配锁
     * @param xid     事务id
     * @param mode    锁类型
     */
    public TxItem(boolean granted, int xid, LockMode mode) {
        this.granted = granted;
        this.xid = xid;
        this.mode = mode;
    }

    /**
     * 请求分配锁
     */
    public void waitForGrant() {
        lock.lock();
        try {
            // 等待分配锁
            while (!this.granted) {
                grantLock.await();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 分配锁
     */
    public void grant() {
        lock.lock();
        try {
            // 分配到锁
            granted = true;
            // 唤醒线程
            grantLock.signal();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 中止请求
     */
    public void abort() {
        lock.lock();
        try {
            grantLock.signal();
        } finally {
            lock.unlock();
        }
    }
}
