package net.kaaass.rumbase.transaction;

import lombok.Getter;
import lombok.Setter;
import net.kaaass.rumbase.transaction.exception.DeadlockException;
import net.kaaass.rumbase.transaction.lock.LockTable;
import net.kaaass.rumbase.transaction.lock.LockTableImpl;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 事务上下文的实现
 *
 * @author criki
 */
public class TransactionContextImpl implements TransactionContext {

    /**
     * 事务Id
     */
    @Getter
    private final int xid;
    /**
     * 事务隔离度
     */
    @Getter
    private final TransactionIsolation isolation;
    /**
     * 存储创建它的管理器
     */
    @Getter
    private final TransactionManager manager;
    /**
     * 事务快照
     */
    @Getter
    private final List<Integer> snapshot;
    /**
     * 状态互斥锁
     */
    private final Lock statusLock = new ReentrantLock();
    /**
     * 事务状态
     */
    @Getter
    @Setter
    private TransactionStatus status;

    /**
     * 事务上下文
     */
    public TransactionContextImpl() {
        this.xid = 0;
        this.status = TransactionStatus.COMMITTED;
        this.isolation = TransactionIsolation.READ_UNCOMMITTED;
        this.manager = null;
        this.snapshot = new ArrayList<>();
    }

    /**
     * 事务上下文
     *
     * @param xid       事务id
     * @param isolation 事务隔离度
     * @param manager   创建事务的管理器
     */
    public TransactionContextImpl(int xid, TransactionIsolation isolation, TransactionManager manager) {
        this.xid = xid;
        this.status = TransactionStatus.PREPARING;
        this.isolation = isolation;
        this.manager = manager;
        this.snapshot = new ArrayList<>();
    }

    /**
     * 事务上下文
     *
     * @param xid       事务id
     * @param isolation 事务隔离度
     * @param manager   创建事务的管理器
     * @param status    事务状态
     */
    public TransactionContextImpl(int xid, TransactionIsolation isolation, TransactionManager manager, TransactionStatus status) {
        this.xid = xid;
        this.isolation = isolation;
        this.manager = manager;
        this.status = status;
        this.snapshot = new ArrayList<>();
    }

    /**
     * 事务上下文
     *
     * @param xid       事务id
     * @param isolation 事务隔离度
     * @param manager   创建事务的管理器
     * @param snapshot  事务状态
     */
    public TransactionContextImpl(int xid, TransactionIsolation isolation, TransactionManager manager, List<Integer> snapshot) {
        this.xid = xid;
        this.isolation = isolation;
        this.manager = manager;
        this.status = TransactionStatus.PREPARING;
        this.snapshot = snapshot;
    }

    /**
     * 事务上下文
     *
     * @param xid       事务id
     * @param isolation 事务隔离度
     * @param manager   创建事务的管理器
     * @param snapshot  事务快照
     * @param status    事务状态
     */
    public TransactionContextImpl(int xid, TransactionIsolation isolation, TransactionManager manager, List<Integer> snapshot, TransactionStatus status) {
        this.xid = xid;
        this.isolation = isolation;
        this.manager = manager;
        this.snapshot = snapshot;
        this.status = status;
    }

    /**
     * 开始事务
     */
    @Override
    public void start() {
        statusLock.lock();
        try {
            if (!this.status.equals(TransactionStatus.PREPARING)) {
                return;
            }
            this.status = TransactionStatus.ACTIVE;
            if (manager != null) {
                manager.changeTransactionStatus(xid, TransactionStatus.ACTIVE);
            }
        } finally {
            statusLock.unlock();
        }
    }

    /**
     * 提交事务
     */
    @Override
    public void commit() {
        statusLock.lock();
        try {
            if (!this.status.equals(TransactionStatus.ACTIVE)) {
                return;
            }
            // 修改状态
            this.status = TransactionStatus.COMMITTED;
            if (manager != null) {
                manager.changeTransactionStatus(xid, TransactionStatus.COMMITTED);
            }

            // 释放锁
            LockTable lockTable = LockTableImpl.getInstance();
            lockTable.release(xid);
        } finally {
            statusLock.unlock();
        }
    }

    /**
     * 中止事务
     */
    @Override
    public void rollback() {
        statusLock.lock();
        try {
            if (!this.status.equals(TransactionStatus.ACTIVE)) {
                return;
            }

            // 修改状态
            this.status = TransactionStatus.ABORTED;
            if (manager != null) {
                manager.changeTransactionStatus(xid, TransactionStatus.ABORTED);
            }

            // 释放锁
            LockTable lockTable = LockTableImpl.getInstance();
            lockTable.release(xid);
        } finally {
            statusLock.unlock();
        }
    }

    /**
     * 加共享锁
     *
     * @param uuid      记录id
     * @param tableName 表字段
     */
    @Override
    public void sharedLock(long uuid, String tableName) throws DeadlockException {
        statusLock.lock();
        try {
            if (!this.status.equals(TransactionStatus.ACTIVE)) {
                return;
            }

            LockTable lockTable = LockTableImpl.getInstance();
            lockTable.addSharedLock(xid, uuid, tableName);
        } finally {
            statusLock.unlock();
        }

    }

    /**
     * 加排他锁
     *
     * @param uuid      记录id
     * @param tableName 表字段
     */
    @Override
    public void exclusiveLock(long uuid, String tableName) throws DeadlockException {
        statusLock.lock();
        try {
            if (!this.status.equals(TransactionStatus.ACTIVE)) {
                return;
            }
            LockTable lockTable = LockTableImpl.getInstance();
            lockTable.addExclusiveLock(xid, uuid, tableName);
        } finally {
            statusLock.unlock();
        }
    }

}
