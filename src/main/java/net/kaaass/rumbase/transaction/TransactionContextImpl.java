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

    @Getter
    private static final List<Integer> SNAPSHOT;

    static {
        SNAPSHOT = new ArrayList<>();
    }

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
    private final TransactionManager manager;
    /**
     * 事务状态
     */
    @Getter
    private TransactionStatus status;

    public TransactionContextImpl() {
        this.xid = 0;
        this.status = TransactionStatus.COMMITTED;
        this.isolation = TransactionIsolation.READ_UNCOMMITTED;
        this.manager = null;
    }

    /**
     * 事务上下文
     *
     * @param isolation 事务隔离度
     * @param manager   创建事务的管理器
     */
    public TransactionContextImpl(int xid, TransactionIsolation isolation, TransactionManager manager) {
        this.xid = xid;
        this.status = TransactionStatus.PREPARING;
        this.isolation = isolation;
        this.manager = manager;
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

        synchronized (SNAPSHOT) {
            SNAPSHOT.add(xid);
        }
    }

    /**
     * 提交事务
     */
    @Override
    public void commit() {
        synchronized (SNAPSHOT) {
            SNAPSHOT.remove(Integer.valueOf(xid));
        }

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
        synchronized (SNAPSHOT) {
            SNAPSHOT.remove(Integer.valueOf(xid));
        }

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
