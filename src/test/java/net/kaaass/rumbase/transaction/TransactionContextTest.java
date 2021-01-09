package net.kaaass.rumbase.transaction;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.transaction.mock.MockTransactionManager;

/**
 * 测试事务上下文
 *
 * @author criki
 */
@Slf4j
public class TransactionContextTest extends TestCase {

    /**
     * 测试创建事务
     */
    public void testCreateTransaction() {
        var manager = new MockTransactionManager();

        //空事务
        var emptyTransaction = TransactionContext.empty();
        assertEquals(0, emptyTransaction.getXid());
        assertEquals(TransactionStatus.COMMITTED, emptyTransaction.getStatus());
        assertEquals(TransactionIsolation.READ_UNCOMMITTED, emptyTransaction.getIsolation());

        //普通事务
        var transaction = manager.createTransactionContext(TransactionIsolation.READ_COMMITTED);
        assertEquals(1, transaction.getXid());
        assertEquals(TransactionStatus.PREPARING, transaction.getStatus());
        assertEquals(TransactionIsolation.READ_COMMITTED, transaction.getIsolation());

    }

    /**
     * 测试事务变化
     */
    public void testChangeStatus() {
        var manager = new MockTransactionManager();
        var committedTransaction = manager.createTransactionContext(TransactionIsolation.READ_COMMITTED);
        //事务初始状态
        assertEquals("new transaction's default status should be PREPARING", TransactionStatus.PREPARING, committedTransaction.getStatus());

        //事务开始
        committedTransaction.start();
        assertEquals("starting transaction's default status should be ACTIVE", TransactionStatus.ACTIVE, committedTransaction.getStatus());

        //事务提交
        committedTransaction.commit();
        assertEquals("committed transaction's default status should be COMMITTED", TransactionStatus.COMMITTED, committedTransaction.getStatus());

        //事务中止
        var abortedTransaction = manager.createTransactionContext(TransactionIsolation.READ_COMMITTED);
        abortedTransaction.start();
        abortedTransaction.rollback();
        assertEquals("aborted transaction's default status should be ABORTED", TransactionStatus.ABORTED, abortedTransaction.getStatus());
    }

    /**
     * 测试事务持久化
     */
    public void testTransactionPersistence() {
        var manager = new MockTransactionManager();
        //事务创建，事务状态记录数改变
        var transaction1 = manager.createTransactionContext(TransactionIsolation.READ_UNCOMMITTED);
        assertEquals(1, MockTransactionManager.TransactionSize);

        var transaction2 = manager.createTransactionContext(TransactionIsolation.READ_UNCOMMITTED);
        assertEquals(2, MockTransactionManager.TransactionSize);

        //事务状态持久化
        assertEquals(TransactionStatus.PREPARING.getStatusId(), (byte) MockTransactionManager.XidLog.get(transaction1.getXid()));
        assertEquals(TransactionStatus.PREPARING.getStatusId(), (byte) MockTransactionManager.XidLog.get(transaction2.getXid()));

        transaction1.start();
        assertEquals(TransactionStatus.ACTIVE.getStatusId(), (byte) MockTransactionManager.XidLog.get(transaction1.getXid()));

        transaction1.commit();
        assertEquals(TransactionStatus.COMMITTED.getStatusId(), (byte) MockTransactionManager.XidLog.get(transaction1.getXid()));

        transaction2.start();
        transaction2.rollback();
        assertEquals(TransactionStatus.ABORTED.getStatusId(), (byte) MockTransactionManager.XidLog.get(transaction2.getXid()));
    }

    /**
     * 测试事务上锁
     */
    public void testAddLock() {
        var manager = new MockTransactionManager();
        var transaction = manager.createTransactionContext(TransactionIsolation.READ_UNCOMMITTED);

        //加共享锁
        transaction.sharedLock(1);

        //加排他锁
        transaction.exclusiveLock(2);

    }
}
