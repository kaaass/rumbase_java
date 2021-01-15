package net.kaaass.rumbase.record;

import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.transaction.TransactionContext;
import net.kaaass.rumbase.transaction.TransactionIsolation;
import net.kaaass.rumbase.transaction.TransactionManager;
import net.kaaass.rumbase.transaction.TransactionStatus;

import java.util.List;

/**
 * 用于测试可见性等的假事务上下文
 */
@Slf4j
@Data
public class FakeTxContext implements TransactionContext {

    private List<Integer> snapshot;

    private TransactionIsolation isolation;

    private TransactionStatus status;

    private TransactionManager manager;

    private int xid;

    @Override
    public void start() {
    }

    @Override
    public void commit() {
        this.status = TransactionStatus.COMMITTED;
    }

    @Override
    public void rollback() {
        this.status = TransactionStatus.ABORTED;
    }

    @Override
    public void sharedLock(long uuid, String tableName) {
        log.info("申请共享锁 {}.{}", tableName, uuid);
    }

    @Override
    public void exclusiveLock(long uuid, String tableName) {
        log.info("申请互斥锁 {}.{}", tableName, uuid);
    }
}
