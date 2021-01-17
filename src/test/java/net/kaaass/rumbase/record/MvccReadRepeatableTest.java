package net.kaaass.rumbase.record;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.FileUtil;
import net.kaaass.rumbase.page.exception.FileException;
import net.kaaass.rumbase.record.exception.NeedRollbackException;
import net.kaaass.rumbase.record.exception.RecordNotFoundException;
import net.kaaass.rumbase.transaction.TransactionContext;
import net.kaaass.rumbase.transaction.TransactionIsolation;
import net.kaaass.rumbase.transaction.TransactionManager;
import net.kaaass.rumbase.transaction.TransactionManagerImpl;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

@Slf4j
public class MvccReadRepeatableTest {

    @BeforeClass
    public static void createDataFolder() {
        FileUtil.prepare();
    }

    @AfterClass
    public static void clearDataFolder() {
        FileUtil.clear();
    }

    public final static String PATH = FileUtil.TEST_PATH;

    @Test
    public void testReadSelf() throws RecordNotFoundException {
        var storage = RecordManager.fromFile(PATH + "testReadSelf");
        var manager = new FakeTxManager(TransactionIsolation.REPEATABLE_READ);
        // 创建事务1
        var tx1 = manager.begin();
        // 事务1的记录自身可见
        var a1 = storage.insert(tx1, new byte[]{0x23, 0x63});
        Assert.assertTrue("tx1 see a1", storage.queryOptional(tx1, a1).isPresent());
        // 事务2不可见a1
        var tx2 = manager.begin();
        Assert.assertTrue("tx2 blind a1", storage.queryOptional(tx2, a1).isEmpty());
        // 事务1新纪录也可见
        for (int i = 0; i < 100; i++) {
            var uuid = storage.insert(tx1, new byte[]{0x23, 0x63, 0x44});
            Assert.assertTrue("uuid see a1", storage.queryOptional(tx1, uuid).isPresent());
        }
    }

    @Test
    public void testReadOther() throws RecordNotFoundException {
        var storage = RecordManager.fromFile(PATH + "testReadOther");
        var manager = new FakeTxManager(TransactionIsolation.REPEATABLE_READ);
        // 创建事务12
        var tx1 = manager.begin();
        var tx2 = manager.begin();
        // 事务2创建的b1，事务1不可见
        var b1 = storage.insert(tx2, new byte[]{0x1, 0x2, 0x3});
        Assert.assertTrue("tx2 see b1", storage.queryOptional(tx2, b1).isPresent());
        Assert.assertTrue("tx1 blind b1", storage.queryOptional(tx1, b1).isEmpty());
        // 事务1创建的a1，事务2不可见
        var a1 = storage.insert(tx1, new byte[]{0x6, 0x5, 0x4, 0x32});
        Assert.assertTrue("tx1 see a1", storage.queryOptional(tx1, a1).isPresent());
        Assert.assertTrue("tx2 blind a1", storage.queryOptional(tx2, a1).isEmpty());
        // 事务2提交，则事务1依旧不可见b1，因为事务1不可见事务2
        tx2.commit();
        Assert.assertTrue("tx1 blind b1 after commit", storage.queryOptional(tx1, b1).isEmpty());
        // 新建事务3
        var tx3 = manager.begin();
        // 事务3可以看到b1，但是不能看到a1
        Assert.assertTrue("tx3 see b1 after commit", storage.queryOptional(tx3, b1).isPresent());
        Assert.assertTrue("tx3 blind a1 before commit", storage.queryOptional(tx3, a1).isEmpty());
        // 提交事务1，a1还是不可见，因为事务3创建时事务1还在运行
        tx1.commit();
        Assert.assertTrue("tx3 blind a1 after commit", storage.queryOptional(tx3, a1).isEmpty());
    }

    @Test
    public void testDelete() throws RecordNotFoundException {
        var storage = RecordManager.fromFile(PATH + "testDelete");
        var manager = new FakeTxManager(TransactionIsolation.REPEATABLE_READ);
        // 创建事务1、记录a1a2
        var tx1 = manager.begin();
        var a1 = storage.insert(tx1, new byte[]{0x1, 0x2, 0x3});
        var a2 = storage.insert(tx1, new byte[]{0x5, 0x6, 0x7});
        Assert.assertTrue(storage.queryOptional(tx1, a1).isPresent());
        Assert.assertTrue(storage.queryOptional(tx1, a2).isPresent());
        // 自身删除
        storage.delete(tx1, a1);
        Assert.assertTrue("a1 should be deleted", storage.queryOptional(tx1, a1).isEmpty());
        // 提交事务1
        tx1.commit();
        // 事务2删除，事务3仍然可见a2
        var tx2 = manager.begin();
        var tx3 = manager.begin();
        storage.delete(tx2, a2);
        Assert.assertTrue("tx2 blind a2 after delete", storage.queryOptional(tx2, a2).isEmpty());
        Assert.assertTrue("tx3 see a2 before commit", storage.queryOptional(tx3, a2).isPresent());
        // 事务2提交，事务3依旧可见a2，因为事务3开始时事务2没有结束
        tx2.commit();
        Assert.assertTrue("tx3 see a2 after commit", storage.queryOptional(tx3, a2).isPresent());
    }

    @Test
    public void testVersionSkip() throws RecordNotFoundException, NeedRollbackException {
        var storage = RecordManager.fromFile(PATH + "testDelete");
        var manager = new FakeTxManager(TransactionIsolation.REPEATABLE_READ);
        // 创建公共版本
        var r = storage.insert(TransactionContext.empty(), new byte[]{0x1, 0x2, 0x3, 0x4});
        // 进行版本跳跃操作
        var tx1 = manager.begin();
        var tx2 = manager.begin();
        Assert.assertTrue(storage.queryOptional(tx1, r).isPresent());
        Assert.assertTrue(storage.queryOptional(tx2, r).isPresent());
        storage.delete(tx1, r);
        tx1.commit();
        try {
            storage.delete(tx2, r);
            Assert.fail("Should rollback tx2 at here");
            tx2.commit();
        } catch (NeedRollbackException e) {
            log.info("Expected runtime exception: ", e);
        }
    }

    @Test
    public void testReadSelfReal() throws RecordNotFoundException, IOException, FileException {
        var storage = RecordManager.fromFile(PATH + "testReadSelfReal");
        var manager = new TransactionManagerImpl();
        // 创建事务1
        var tx1 = manager.createTransactionContext(TransactionIsolation.REPEATABLE_READ);
        tx1.start();
        // 事务1的记录自身可见
        var a1 = storage.insert(tx1, new byte[]{0x23, 0x63});
        Assert.assertTrue("tx1 see a1", storage.queryOptional(tx1, a1).isPresent());
        // 事务2不可见a1
        var tx2 = manager.createTransactionContext(TransactionIsolation.REPEATABLE_READ);
        tx2.start();
        Assert.assertTrue("tx2 blind a1", storage.queryOptional(tx2, a1).isEmpty());
        // 事务1新纪录也可见
        for (int i = 0; i < 100; i++) {
            var uuid = storage.insert(tx1, new byte[]{0x23, 0x63, 0x44});
            Assert.assertTrue("uuid see a1", storage.queryOptional(tx1, uuid).isPresent());
        }
        //
        tx1.commit();
        tx2.commit();
    }

    @Test
    public void testReadOtherReal() throws RecordNotFoundException, IOException, FileException {
        var storage = RecordManager.fromFile(PATH + "testReadOtherReal");
        var manager = new TransactionManagerImpl();
        // 创建事务12
        var tx1 = manager.createTransactionContext(TransactionIsolation.REPEATABLE_READ);
        tx1.start();
        var tx2 = manager.createTransactionContext(TransactionIsolation.REPEATABLE_READ);
        tx2.start();
        // 事务2创建的b1，事务1不可见
        var b1 = storage.insert(tx2, new byte[]{0x1, 0x2, 0x3});
        Assert.assertTrue("tx2 see b1", storage.queryOptional(tx2, b1).isPresent());
        Assert.assertTrue("tx1 blind b1", storage.queryOptional(tx1, b1).isEmpty());
        // 事务1创建的a1，事务2不可见
        var a1 = storage.insert(tx1, new byte[]{0x6, 0x5, 0x4, 0x32});
        Assert.assertTrue("tx1 see a1", storage.queryOptional(tx1, a1).isPresent());
        Assert.assertTrue("tx2 blind a1", storage.queryOptional(tx2, a1).isEmpty());
        // 事务2提交，则事务1依旧不可见b1，因为事务1不可见事务2
        tx2.commit();
        Assert.assertTrue("tx1 blind b1 after commit", storage.queryOptional(tx1, b1).isEmpty());
        // 新建事务3
        var tx3 = manager.createTransactionContext(TransactionIsolation.REPEATABLE_READ);
        tx3.start();
        // 事务3可以看到b1，但是不能看到a1
        Assert.assertTrue("tx3 see b1 after commit", storage.queryOptional(tx3, b1).isPresent());
        Assert.assertTrue("tx3 blind a1 before commit", storage.queryOptional(tx3, a1).isEmpty());
        // 提交事务1，a1还是不可见，因为事务3创建时事务1还在运行
        tx1.commit();
        Assert.assertTrue("tx3 blind a1 after commit", storage.queryOptional(tx3, a1).isEmpty());
        //
        tx3.commit();
    }

    @Test
    public void testDeleteReal() throws RecordNotFoundException, IOException, FileException {
        var storage = RecordManager.fromFile(PATH + "testDeleteReal");
        var manager = new TransactionManagerImpl();
        // 创建事务1、记录a1a2
        var tx1 = manager.createTransactionContext(TransactionIsolation.REPEATABLE_READ);
        tx1.start();
        var a1 = storage.insert(tx1, new byte[]{0x1, 0x2, 0x3});
        var a2 = storage.insert(tx1, new byte[]{0x5, 0x6, 0x7});
        Assert.assertTrue(storage.queryOptional(tx1, a1).isPresent());
        Assert.assertTrue(storage.queryOptional(tx1, a2).isPresent());
        // 自身删除
        storage.delete(tx1, a1);
        Assert.assertTrue("a1 should be deleted", storage.queryOptional(tx1, a1).isEmpty());
        // 提交事务1
        tx1.commit();
        // 事务2删除，事务3仍然可见a2
        var tx2 = manager.createTransactionContext(TransactionIsolation.REPEATABLE_READ);
        tx2.start();
        var tx3 = manager.createTransactionContext(TransactionIsolation.REPEATABLE_READ);
        tx3.start();
        storage.delete(tx2, a2);
        Assert.assertTrue("tx2 blind a2 after delete", storage.queryOptional(tx2, a2).isEmpty());
        Assert.assertTrue("tx3 see a2 before commit", storage.queryOptional(tx3, a2).isPresent());
        // 事务2提交，事务3依旧可见a2，因为事务3开始时事务2没有结束
        tx2.commit();
        Assert.assertTrue("tx3 see a2 after commit", storage.queryOptional(tx3, a2).isPresent());
        //
        tx3.commit();
    }

    @Test
    public void testVersionSkipReal() throws RecordNotFoundException, NeedRollbackException, IOException, FileException {
        var storage = RecordManager.fromFile(PATH + "testDeleteReal");
        var manager = new TransactionManagerImpl();
        // 创建公共版本
        var r = storage.insert(TransactionContext.empty(), new byte[]{0x1, 0x2, 0x3, 0x4});
        // 进行版本跳跃操作
        var tx1 = manager.createTransactionContext(TransactionIsolation.REPEATABLE_READ);
        tx1.start();
        var tx2 = manager.createTransactionContext(TransactionIsolation.REPEATABLE_READ);
        tx2.start();
        Assert.assertTrue(storage.queryOptional(tx1, r).isPresent());
        Assert.assertTrue(storage.queryOptional(tx2, r).isPresent());
        storage.delete(tx1, r);
        tx1.commit();
        try {
            storage.delete(tx2, r);
            Assert.fail("Should rollback tx2 at here");
            tx2.commit();
        } catch (NeedRollbackException e) {
            log.info("Expected runtime exception: ", e);
            tx2.rollback();
        }
    }
}
