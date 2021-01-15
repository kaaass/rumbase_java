package net.kaaass.rumbase.record;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.record.exception.RecordNotFoundException;
import net.kaaass.rumbase.transaction.TransactionIsolation;

@Slf4j
public class MvccReadCommitTest extends TestCase {

    public void testReadSelf() throws RecordNotFoundException {
        var storage = RecordManager.fromFile("MvccReadCommitTest::testReadSelf");
        var manager = new FakeTxManager(TransactionIsolation.READ_COMMITTED);
        // 创建事务1
        var tx1 = manager.begin();
        // 事务1的记录自身可见
        var a1 = storage.insert(tx1, new byte[]{0x23, 0x63});
        assertTrue("tx1 see a1", storage.queryOptional(tx1, a1).isPresent());
        // 事务2不可见a1
        var tx2 = manager.begin();
        assertTrue("tx2 blind a1", storage.queryOptional(tx2, a1).isEmpty());
        // 事务1新纪录也可见
        for (int i = 0; i < 100; i++) {
            var uuid = storage.insert(tx1, new byte[]{0x23, 0x63, 0x44});
            assertTrue("uuid see a1", storage.queryOptional(tx1, uuid).isPresent());
        }
    }

    public void testReadOther() throws RecordNotFoundException {
        var storage = RecordManager.fromFile("MvccReadCommitTest::testReadOther");
        var manager = new FakeTxManager(TransactionIsolation.READ_COMMITTED);
        // 创建事务12
        var tx1 = manager.begin();
        var tx2 = manager.begin();
        // 事务2创建的b1，事务1不可见
        var b1 = storage.insert(tx2, new byte[]{0x1, 0x2, 0x3});
        assertTrue("tx2 see b1", storage.queryOptional(tx2, b1).isPresent());
        assertTrue("tx1 blind b1", storage.queryOptional(tx1, b1).isEmpty());
        // 事务1创建的a1，事务2不可见
        var a1 = storage.insert(tx1, new byte[]{0x6, 0x5, 0x4, 0x32});
        assertTrue("tx1 see a1", storage.queryOptional(tx1, a1).isPresent());
        assertTrue("tx2 blind a1", storage.queryOptional(tx2, a1).isEmpty());
        // 事务2提交，则事务1可见b1
        tx2.commit();
        assertTrue("tx1 see b1 after commit", storage.queryOptional(tx1, b1).isPresent());
        // 新建事务3
        var tx3 = manager.begin();
        // 事务3可以看到b1，但是不能看到a1
        assertTrue("tx3 see b1 after commit", storage.queryOptional(tx3, b1).isPresent());
        assertTrue("tx3 blind a1 before commit", storage.queryOptional(tx3, a1).isEmpty());
        // 提交事务1，a1可见
        tx1.commit();
        assertTrue("tx3 see a1 after commit", storage.queryOptional(tx3, a1).isPresent());
    }

    public void testDelete() throws RecordNotFoundException {
        var storage = RecordManager.fromFile("MvccReadCommitTest::testDelete");
        var manager = new FakeTxManager(TransactionIsolation.READ_COMMITTED);
        // 创建事务1、记录a1a2
        var tx1 = manager.begin();
        var a1 = storage.insert(tx1, new byte[]{0x1, 0x2, 0x3});
        var a2 = storage.insert(tx1, new byte[]{0x5, 0x6, 0x7});
        assertTrue(storage.queryOptional(tx1, a1).isPresent());
        assertTrue(storage.queryOptional(tx1, a2).isPresent());
        // 自身删除
        storage.delete(tx1, a1);
        assertTrue("a1 should be deleted", storage.queryOptional(tx1, a1).isEmpty());
        // 提交事务1
        tx1.commit();
        // 事务2删除，事务3仍然可见a2
        var tx2 = manager.begin();
        var tx3 = manager.begin();
        storage.delete(tx2, a2);
        assertTrue("tx2 blind a2 after delete", storage.queryOptional(tx2, a2).isEmpty());
        assertTrue("tx3 see a2 before commit", storage.queryOptional(tx3, a2).isPresent());
        // 事务2提交，事务3不可见a2
        tx2.commit();
        assertTrue("tx3 blind a2 after commit", storage.queryOptional(tx3, a2).isEmpty());
    }
}
