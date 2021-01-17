package net.kaaass.rumbase.transaction;

import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.FileUtil;
import net.kaaass.rumbase.page.exception.FileException;
import net.kaaass.rumbase.transaction.exception.DeadlockException;
import net.kaaass.rumbase.transaction.exception.StatusException;
import org.junit.*;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import static junit.framework.TestCase.assertTrue;

/**
 * 测试事务上下文
 *
 * @author criki
 */
@Slf4j
public class TransactionContextTest {

    @BeforeClass
    public static void createDataFolder() {
        log.info("创建测试文件夹...");
        FileUtil.createDir(FileUtil.TEST_PATH);
    }

    @AfterClass
    public static void clearDataFolder() {
        log.info("清除测试文件夹...");
        FileUtil.removeDir(FileUtil.TEST_PATH);
    }

    /**
     * 测试创建事务
     */
    @Test
    public void testCreateTransaction() throws IOException, FileException {
        var manager = new TransactionManagerImpl("test_gen_files/test_create.log");

        // 空事务
        var emptyTransaction = TransactionContext.empty();
        Assert.assertEquals(0, emptyTransaction.getXid());
        Assert.assertEquals(TransactionStatus.COMMITTED, emptyTransaction.getStatus());
        Assert.assertEquals(TransactionIsolation.READ_UNCOMMITTED, emptyTransaction.getIsolation());

        // 普通事务
        var transaction = manager.createTransactionContext(TransactionIsolation.READ_COMMITTED);
        Assert.assertEquals(1, transaction.getXid());
        Assert.assertEquals(TransactionStatus.PREPARING, transaction.getStatus());
        Assert.assertEquals(TransactionIsolation.READ_COMMITTED, transaction.getIsolation());
    }

    /**
     * 测试事务变化
     */
    @Test
    public void testChangeStatus() throws IOException, FileException, StatusException {
        var manager = new TransactionManagerImpl("test_gen_files/test_change.log");
        var committedTransaction = manager.createTransactionContext(TransactionIsolation.READ_COMMITTED);
        // 事务初始状态
        Assert.assertEquals("new transaction's default status should be PREPARING", TransactionStatus.PREPARING, committedTransaction.getStatus());

        // 事务开始
        committedTransaction.start();
        Assert.assertEquals("starting transaction's default status should be ACTIVE", TransactionStatus.ACTIVE, committedTransaction.getStatus());

        // 事务提交
        committedTransaction.commit();
        Assert.assertEquals("committed transaction's default status should be COMMITTED", TransactionStatus.COMMITTED, committedTransaction.getStatus());

        // 事务中止
        var abortedTransaction = manager.createTransactionContext(TransactionIsolation.READ_COMMITTED);
        abortedTransaction.start();
        abortedTransaction.rollback();
        Assert.assertEquals("aborted transaction's default status should be ABORTED", TransactionStatus.ABORTED, abortedTransaction.getStatus());
    }

    /**
     * 测试事务持久化
     */
    @Test
    public void testTransactionPersistence() throws IOException, FileException, StatusException {
        var manager = new TransactionManagerImpl("test_gen_files/test_persistence.log");
        // 事务创建，事务状态记录数改变
        var transaction1 = manager.createTransactionContext(TransactionIsolation.READ_UNCOMMITTED);
        Assert.assertEquals(1, manager.getSIZE().get());

        var transaction2 = manager.createTransactionContext(TransactionIsolation.READ_UNCOMMITTED);
        Assert.assertEquals(2, manager.getSIZE().get());

        // 事务状态持久化
        var txFromDisk1 = manager.getContext(1);
        var txFromDisk2 = manager.getContext(2);
        Assert.assertEquals(TransactionStatus.PREPARING, txFromDisk1.getStatus());
        Assert.assertEquals(TransactionStatus.PREPARING, txFromDisk2.getStatus());

        transaction1.start();
        txFromDisk1 = manager.getContext(1);
        Assert.assertEquals(TransactionStatus.ACTIVE, txFromDisk1.getStatus());

        transaction1.commit();
        txFromDisk1 = manager.getContext(1);
        Assert.assertEquals(TransactionStatus.COMMITTED, txFromDisk1.getStatus());

        transaction2.start();
        transaction2.rollback();
        txFromDisk2 = manager.getContext(2);
        Assert.assertEquals(TransactionStatus.ABORTED, txFromDisk2.getStatus());
    }

    /**
     * 测试事务状态复原
     */
    @Test
    public void testTransactionRecovery() throws IOException, FileException, StatusException {
        var manager = new TransactionManagerImpl("test_gen_files/test_recovery.log");
        // 事务创建，事务状态记录数改变
        var transaction1 = manager.createTransactionContext(TransactionIsolation.READ_UNCOMMITTED);
        int xid = transaction1.getXid();

        // 复原事务
        var transactionR = manager.getContext(xid);
        Assert.assertEquals(TransactionStatus.PREPARING, transactionR.getStatus());
        Assert.assertEquals(TransactionIsolation.READ_UNCOMMITTED, transactionR.getIsolation());

        // 改变事务状态
        transaction1.start();
        transactionR = manager.getContext(xid);
        Assert.assertEquals(TransactionStatus.ACTIVE, transactionR.getStatus());
    }

    /**
     * 测试事务上锁
     */
    @Test
    public void testAddLock() throws IOException, FileException, StatusException {
        var manager = new TransactionManagerImpl("test_gen_files/test_add_lock.log");
        var transaction1 = manager.createTransactionContext(TransactionIsolation.READ_UNCOMMITTED);
        var transaction2 = manager.createTransactionContext(TransactionIsolation.READ_UNCOMMITTED);
        String tableName = "test";

        transaction1.start();
        transaction2.start();

        // 互斥锁
        new Thread(() -> {
            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            log.info("transaction2 commit");
            try {
                transaction2.commit();
            } catch (StatusException e) {
                e.printStackTrace();
            }
        }).start();
        try {
            transaction1.exclusiveLock(1, tableName);
            transaction2.exclusiveLock(2, tableName);
            transaction1.exclusiveLock(2, tableName);
            log.info("transaction1 got exclusiveLock on 2");
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
        } catch (DeadlockException | StatusException e) {
            e.printStackTrace();
        }
    }

    /**
     * 测试死锁
     */
    @Test
    public void testDeadlock() throws IOException, FileException, InterruptedException, StatusException {
        var manager = new TransactionManagerImpl("test_gen_files/test_deadlock.log");
        for (int i = 0; i < 5; i++) {
            log.info("============= Test times {} =============", i);
            var transaction1 = manager.createTransactionContext(TransactionIsolation.READ_UNCOMMITTED);
            var transaction2 = manager.createTransactionContext(TransactionIsolation.READ_UNCOMMITTED);
            String tableName = "test";

            transaction1.start();
            transaction2.start();

            AtomicBoolean syncPoint = new AtomicBoolean(false);
            AtomicBoolean deadlockDetect = new AtomicBoolean(false);
            Thread thread = new Thread(() -> {
                try {
                    while (!syncPoint.get()) {
                        Thread.sleep(10);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                try {
                    transaction2.exclusiveLock(1, tableName);
                } catch (DeadlockException e) {
                    deadlockDetect.set(true);
                    e.printStackTrace();
                    try {
                        transaction2.rollback();
                    } catch (StatusException statusException) {
                        statusException.printStackTrace();
                    }
                } catch (StatusException e) {
                    e.printStackTrace();
                }
            });
            thread.start();
            try {
                transaction1.exclusiveLock(1, tableName);
                transaction2.exclusiveLock(2, tableName);
                syncPoint.set(true);
                transaction1.exclusiveLock(2, tableName);
            } catch (DeadlockException e) {
                deadlockDetect.set(true);
                e.printStackTrace();
                log.info("rollback tx2");
                transaction2.rollback();
            }
            thread.join();
            assertTrue("Deadlock should be detected", deadlockDetect.get());
            transaction1.commit();
            log.info("tx1 committed");
        }
    }

    @Ignore
    public void testDeadlock3() throws IOException, FileException, InterruptedException, StatusException {
        // FIXME 三线程死锁问题仍不能解决
        var manager = new TransactionManagerImpl("test_gen_files/test_deadlock3.log");
        for (int i = 0; i < 1; i++) {
            log.info("============= Test times {} =============", i);
            var tx1 = manager.createTransactionContext(TransactionIsolation.READ_UNCOMMITTED);
            var tx2 = manager.createTransactionContext(TransactionIsolation.READ_UNCOMMITTED);
            var tx3 = manager.createTransactionContext(TransactionIsolation.READ_UNCOMMITTED);
            String tableName = "test";

            tx1.start();
            tx2.start();
            tx3.start();

            AtomicBoolean syncPoint = new AtomicBoolean(false);
            AtomicBoolean deadlockDetect = new AtomicBoolean(false);
            var thread2 = new Thread(() -> {
                try {
                    while (!syncPoint.get()) {
                        Thread.sleep(3);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                try {
                    tx2.exclusiveLock(3, tableName);
                } catch (DeadlockException e) {
                    deadlockDetect.set(true);
                    e.printStackTrace();
                    try {
                        log.info("rollback tx2");
                        tx2.rollback();
                    } catch (StatusException statusException) {
                        statusException.printStackTrace();
                    }
                } catch (StatusException e) {
                    e.printStackTrace();
                }
                log.info("thread2 alive");
            });
            var thread3 = new Thread(() -> {
                try {
                    while (!syncPoint.get()) {
                        Thread.sleep(3);
                    }
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                try {
                    tx3.exclusiveLock(1, tableName);
                } catch (DeadlockException e) {
                    deadlockDetect.set(true);
                    e.printStackTrace();
                    try {
                        log.info("rollback tx3");
                        tx3.rollback();
                    } catch (StatusException statusException) {
                        statusException.printStackTrace();
                    }
                } catch (StatusException e) {
                    e.printStackTrace();
                }
                log.info("thread3 alive");
            });
            thread2.start();
            thread3.start();
            try {
                tx1.exclusiveLock(1, tableName);
                tx2.exclusiveLock(2, tableName);
                tx3.exclusiveLock(3, tableName);
                syncPoint.set(true);
                tx1.exclusiveLock(2, tableName);
            } catch (DeadlockException e) {
                deadlockDetect.set(true);
                e.printStackTrace();
                log.info("rollback tx1");
                tx1.rollback();
            }
            log.info("thread1 alive");
            thread2.join();
            thread3.join();
            assertTrue("Deadlock should be detected", deadlockDetect.get());
            if (tx1.getStatus() == TransactionStatus.ACTIVE) {
                tx1.commit();
                log.info("tx1 committed");
            }
            if (tx2.getStatus() == TransactionStatus.ACTIVE) {
                tx2.commit();
                log.info("tx2 committed");
            }
            if (tx3.getStatus() == TransactionStatus.ACTIVE) {
                tx3.commit();
                log.info("tx3 committed");
            }
        }
    }
}
