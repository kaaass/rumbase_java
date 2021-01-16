package net.kaaass.rumbase.recovery;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.dataitem.IItemStorage;
import net.kaaass.rumbase.dataitem.ItemManager;
import net.kaaass.rumbase.dataitem.exception.UUIDException;
import net.kaaass.rumbase.page.exception.FileException;
import net.kaaass.rumbase.page.exception.PageException;
import net.kaaass.rumbase.recovery.exception.LogException;
import net.kaaass.rumbase.transaction.TransactionContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;

/**
 * 对日志进行保存和恢复
 */
@Slf4j
public class IRecoveryTest extends TestCase {

    public void testInsert() throws PageException, LogException, FileException, IOException, UUIDException {
        String fileName = "testInsert.db";
        IItemStorage iItemStorage = ItemManager.fromFile(fileName);
        byte[] bytes = new byte[]{1, 2, 3, 4};
        var txContext = TransactionContext.empty();
        var recoveryStorage = iItemStorage.getRecoveryStorage();
        var xid = 1;
        var xid2 = 0;
        List<Integer> snaps = new ArrayList<>();
        // 测试日志中存在，但是表中不存在进行恢复
        recoveryStorage.begin(xid,snaps);
        long a = 1;
        long uuid = (a << 32) + Math.abs(new Random().nextInt());
        recoveryStorage.insert(xid,uuid,bytes);
        recoveryStorage.commit(xid);

        // 测试日志中存在，且数据中也存在，会不会重复插入

        recoveryStorage.begin(xid2,snaps);
        iItemStorage.insertItem(txContext,bytes);
        recoveryStorage.commit(xid2);

        recoveryStorage.recovery();
        // 测试日志有额外的数据有没有被插入
        var result = iItemStorage.queryItemByUuid(uuid);
        assertTrue(Arrays.equals(result,bytes));
        // 测试表中有的数据是否被重复插入
        var list = iItemStorage.listItemByPageId(1);
        assertEquals(2,list.size());
    }

    public void testUpdate() throws PageException, LogException, FileException, IOException, UUIDException {
        String fileName = "testUpdate.db";
        IItemStorage iItemStorage = ItemManager.fromFile(fileName);
        byte[] bytes = new byte[]{1, 2, 3, 4};
        byte[] bytesUpdate = new byte[]{2,3,4,5};
        var txContext = TransactionContext.empty();
        var recoveryStorage = iItemStorage.getRecoveryStorage();

        int xid = 0;
        List<Integer> snaps = new ArrayList<>();

        // 测试在表中存在的redo
        recoveryStorage.begin(xid,snaps);
        long uuid  = iItemStorage.insertItem(txContext,bytes);
        iItemStorage.updateItemByUuid(txContext,uuid,bytesUpdate);
        recoveryStorage.commit(xid);

        // 测试在表中不存在的redo
        int xid2 = 1;
        recoveryStorage.begin(xid2,snaps);
        long a = 1;
        long uuid2 = (a << 32) + Math.abs(new Random().nextInt());
        recoveryStorage.insert(xid2,uuid2,bytes);
        recoveryStorage.update(xid2,uuid2,bytes,bytesUpdate);
        recoveryStorage.commit(xid2);

        // 测试abort的事务
        int xid3 = 2;
        recoveryStorage.begin(xid3,snaps);
        long b = 1;
        long uuid3 = (b << 32) + Math.abs(new Random().nextInt());
        recoveryStorage.insert(xid3,uuid3,bytes);
        recoveryStorage.commit(xid3);
        int xid4 = 3;
        recoveryStorage.begin(xid4,snaps);
        recoveryStorage.update(xid4,uuid3,bytes,bytesUpdate);
        recoveryStorage.rollback(xid4);

        recoveryStorage.recovery();

        assertTrue(Arrays.equals(bytesUpdate,iItemStorage.queryItemByUuid(uuid)));
        assertTrue(Arrays.equals(bytesUpdate,iItemStorage.queryItemByUuid(uuid2)));
        assertTrue(Arrays.equals(bytes,iItemStorage.queryItemByUuid(uuid3)));
        var list = iItemStorage.listItemByPageId(1);
        assertEquals(3,list.size());
    }

    public void testUpdateMeta() throws PageException, LogException, FileException, IOException {
        String fileName = "testUpdateMeta.db";
        IItemStorage iItemStorage = ItemManager.fromFile(fileName);
        byte[] bytes = new byte[]{1, 2, 3, 4};
        byte[] bytesUpdate = new byte[]{2,3,4,5};
        var txContext = TransactionContext.empty();
        var recoveryStorage = iItemStorage.getRecoveryStorage();

        // 测试表中有
        int xid = 0;
        List<Integer> snaps = new ArrayList<>();
        recoveryStorage.begin(xid,snaps);
        var uuid = iItemStorage.setMetadata(txContext,bytes);
        recoveryStorage.commit(xid);

        assertTrue(Arrays.equals(bytes,iItemStorage.getMetadata()));
        // 测试表中没有头信息时更新
        int xid2 = 1;
        recoveryStorage.begin(xid2,snaps);
        recoveryStorage.updateMeta(xid2,uuid,bytesUpdate);
        recoveryStorage.commit(xid2);
        recoveryStorage.recovery();
        assertTrue(Arrays.equals(bytesUpdate,iItemStorage.getMetadata()));

    }

    public void testUpdateMetaFail() throws PageException, LogException, FileException, IOException {
        String fileName = "testUpdateMetaFail.db";
        IItemStorage iItemStorage = ItemManager.fromFile(fileName);
        byte[] bytes = new byte[]{1, 2, 3, 4};
        byte[] bytesUpdate = new byte[]{2,3,4,5};
        var txContext = TransactionContext.empty();
        var recoveryStorage = iItemStorage.getRecoveryStorage();

        // 测试表中有
        int xid = 0;
        List<Integer> snaps = new ArrayList<>();
        recoveryStorage.begin(xid,snaps);
        var uuid = iItemStorage.setMetadata(txContext,bytes);
        recoveryStorage.commit(xid);

        assertTrue(Arrays.equals(bytes,iItemStorage.getMetadata()));
        // 测试事务失败
        int xid2 = 1;
        recoveryStorage.begin(xid2,snaps);
        recoveryStorage.updateMeta(xid2,uuid,bytesUpdate);
        recoveryStorage.rollback(xid2);
        recoveryStorage.recovery();
        assertTrue(Arrays.equals(bytes,iItemStorage.getMetadata()));

    }


}
