package net.kaaass.rumbase.transaction.mock;

import lombok.Getter;
import net.kaaass.rumbase.transaction.TransactionContext;
import net.kaaass.rumbase.transaction.TransactionIsolation;
import net.kaaass.rumbase.transaction.TransactionManager;
import net.kaaass.rumbase.transaction.TransactionStatus;

import java.util.ArrayList;
import java.util.List;

/**
 * Mock事务上下文
 *
 * @author criki
 */
@Deprecated
public class MockTransactionContext implements TransactionContext {

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

    public MockTransactionContext() {
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
    public MockTransactionContext(int xid, TransactionIsolation isolation, TransactionManager manager) {
        this.xid = xid;
        this.status = TransactionStatus.PREPARING;
        this.isolation = isolation;
        this.manager = manager;
    }

    @Override
    public void start() {
        this.status = TransactionStatus.ACTIVE;
        if (manager != null) {
            manager.changeTransactionStatus(xid, TransactionStatus.ACTIVE);
        }
    }

    @Override
    public void commit() {
        this.status = TransactionStatus.COMMITTED;
        if (manager != null) {
            manager.changeTransactionStatus(xid, TransactionStatus.COMMITTED);
        }
    }

    @Override
    public void rollback() {
        this.status = TransactionStatus.ABORTED;
        if (manager != null) {
            manager.changeTransactionStatus(xid, TransactionStatus.ABORTED);
        }
    }

    @Override
    public void sharedLock(long uuid, String tableName) {

    }

    @Override
    public void exclusiveLock(long uuid, String tableName) {

    }
}