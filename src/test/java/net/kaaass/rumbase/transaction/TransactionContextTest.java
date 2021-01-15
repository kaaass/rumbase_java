package net.kaaass.rumbase.transaction;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.page.exception.FileException;
import net.kaaass.rumbase.transaction.exception.DeadlockException;

import java.io.IOException;

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
    public void testCreateTransaction() throws IOException, FileException {
        var manager = new TransactionManagerImpl();

        // 空事务
        var emptyTransaction = TransactionContext.empty();
        assertEquals(0, emptyTransaction.getXid());
        assertEquals(TransactionStatus.COMMITTED, emptyTransaction.getStatus());
        assertEquals(TransactionIsolation.READ_UNCOMMITTED, emptyTransaction.getIsolation());

        // 普通事务
        var transaction = manager.createTransactionContext(TransactionIsolation.READ_COMMITTED);
        assertEquals(1, transaction.getXid());
        assertEquals(TransactionStatus.PREPARING, transaction.getStatus());
        assertEquals(TransactionIsolation.READ_COMMITTED, transaction.getIsolation());

    }

    /**
     * 测试事务变化
     */
    public void testChangeStatus() throws IOException, FileException {
        // TODO 将Mock类改成实现类
        var manager = new TransactionManagerImpl();
        var committedTransaction = manager.createTransactionContext(TransactionIsolation.READ_COMMITTED);
        // 事务初始状态
        assertEquals("new transaction's default status should be PREPARING", TransactionStatus.PREPARING, committedTransaction.getStatus());

        // 事务开始
        committedTransaction.start();
        assertEquals("starting transaction's default status should be ACTIVE", TransactionStatus.ACTIVE, committedTransaction.getStatus());

        // 事务提交
        committedTransaction.commit();
        assertEquals("committed transaction's default status should be COMMITTED", TransactionStatus.COMMITTED, committedTransaction.getStatus());

        // 事务中止
        var abortedTransaction = manager.createTransactionContext(TransactionIsolation.READ_COMMITTED);
        abortedTransaction.start();
        abortedTransaction.rollback();
        assertEquals("aborted transaction's default status should be ABORTED", TransactionStatus.ABORTED, abortedTransaction.getStatus());
    }

    /**
     * 测试事务持久化
     */
    public void testTransactionPersistence() throws IOException, FileException {
        // TODO 将Mock类改成实现类
        var manager = new TransactionManagerImpl();
        // 事务创建，事务状态记录数改变
        var transaction1 = manager.createTransactionContext(TransactionIsolation.READ_UNCOMMITTED);
        assertEquals(1, manager.getSIZE().get());

        var transaction2 = manager.createTransactionContext(TransactionIsolation.READ_UNCOMMITTED);
        assertEquals(2, manager.getSIZE().get());

        // 事务状态持久化
        var txFromDisk1 = manager.getContext(1);
        var txFromDisk2 = manager.getContext(2);
        assertEquals(TransactionStatus.PREPARING, txFromDisk1.getStatus());
        assertEquals(TransactionStatus.PREPARING, txFromDisk2.getStatus());

        transaction1.start();
        txFromDisk1 = manager.getContext(1);
        assertEquals(TransactionStatus.ACTIVE, txFromDisk1.getStatus());

        transaction1.commit();
        txFromDisk1 = manager.getContext(1);
        assertEquals(TransactionStatus.COMMITTED, txFromDisk1.getStatus());

        transaction2.start();
        transaction2.rollback();
        txFromDisk2 = manager.getContext(2);
        assertEquals(TransactionStatus.ABORTED, txFromDisk2.getStatus());
    }

    /**
     * 测试事务状态复原
     */
    public void testTransactionRecovery() throws IOException, FileException {
        var manager = new TransactionManagerImpl();
        // 事务创建，事务状态记录数改变
        var transaction1 = manager.createTransactionContext(TransactionIsolation.READ_UNCOMMITTED);
        int xid = transaction1.getXid();

        // 复原事务
        var transactionR = manager.getContext(xid);
        assertEquals(TransactionStatus.PREPARING, transactionR.getStatus());
        assertEquals(TransactionIsolation.READ_UNCOMMITTED, transactionR.getIsolation());

        // 改变事务状态
        transaction1.start();
        transactionR = manager.getContext(xid);
        assertEquals(TransactionStatus.ACTIVE, transactionR.getStatus());
    }

    /**
     * 测试事务上锁
     */
    public void testAddLock() throws IOException, FileException {
        // TODO 将Mock类改成实现类
        var manager = new TransactionManagerImpl();
        var transaction1 = manager.createTransactionContext(TransactionIsolation.READ_UNCOMMITTED);
        var transaction2 = manager.createTransactionContext(TransactionIsolation.READ_UNCOMMITTED);
        String tableName = "test";


        // 互斥锁
        new Thread(() -> {
            try {
                Thread.sleep(30);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            transaction2.commit();
        }).start();
        try {
            transaction1.exclusiveLock(1, tableName);
            transaction2.exclusiveLock(2, tableName);
            transaction1.exclusiveLock(2, tableName);
        } catch (DeadlockException e) {
            e.printStackTrace();
        }
        transaction1.commit();

        try {
            transaction1.sharedLock(1, tableName);
            transaction2.sharedLock(1, tableName);
            transaction2.sharedLock(2, tableName);
            transaction1.sharedLock(2, tableName);

            transaction1.commit();
            transaction2.rollback();
        } catch (DeadlockException e) {
            e.printStackTrace();
        }
    }

    /**
     * 测试死锁
     */
    public void testDeadlock() throws IOException, FileException {
        var manager = new TransactionManagerImpl();
        var transaction1 = manager.createTransactionContext(TransactionIsolation.READ_UNCOMMITTED);
        var transaction2 = manager.createTransactionContext(TransactionIsolation.READ_UNCOMMITTED);
        String tableName = "test";


        new Thread(() -> {
            try {
                Thread.sleep(3);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            try {
                transaction2.exclusiveLock(1, tableName);
            } catch (DeadlockException e) {
                transaction2.rollback();
                e.printStackTrace();
            }
        }).start();
        try {
            transaction1.exclusiveLock(1, tableName);
            transaction2.exclusiveLock(2, tableName);
            transaction1.exclusiveLock(2, tableName);
        } catch (DeadlockException e) {
            e.printStackTrace();
        }
    }
}
