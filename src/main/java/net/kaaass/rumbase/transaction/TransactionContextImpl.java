package net.kaaass.rumbase.transaction;

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;

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
     * 事务状态
     */
    @Getter
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
        this.status = TransactionStatus.ACTIVE;
        if (manager != null) {
            manager.changeTransactionStatus(xid, TransactionStatus.ACTIVE);
        }

    }

    /**
     * 提交事务
     */
    @Override
    public void commit() {
        this.status = TransactionStatus.COMMITTED;
        if (manager != null) {
            manager.changeTransactionStatus(xid, TransactionStatus.COMMITTED);
        }
    }

    /**
     * 中止事务
     */
    @Override
    public void rollback() {
        this.status = TransactionStatus.ABORTED;
        if (manager != null) {
            manager.changeTransactionStatus(xid, TransactionStatus.ABORTED);
        }
    }

    /**
     * 加共享锁
     *
     * @param uuid      记录id
     * @param tableName 表字段
     */
    @Override
    public void sharedLock(long uuid, String tableName) {
        //TODO 加锁
    }

    /**
     * 加排他锁
     *
     * @param uuid      记录id
     * @param tableName 表字段
     */
    @Override
    public void exclusiveLock(long uuid, String tableName) {
        //TODO 加锁
    }

}
