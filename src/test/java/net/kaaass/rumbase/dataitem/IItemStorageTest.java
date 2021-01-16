package net.kaaass.rumbase.dataitem;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.dataitem.exception.PageCorruptedException;
import net.kaaass.rumbase.dataitem.exception.UUIDException;
import net.kaaass.rumbase.page.exception.FileException;
import net.kaaass.rumbase.page.exception.PageException;
import net.kaaass.rumbase.recovery.exception.LogException;
import net.kaaass.rumbase.transaction.TransactionContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;

/**
 * 对数据项管理部分进行测试
 *
 * @author kaito
 * @see net.kaaass.rumbase.dataitem.IItemStorage
 */

@Slf4j
public class IItemStorageTest extends TestCase {

    /**
     * 测试能否从已有文件中解析得到数据项管理器
     */
    public void testGetFromFile() throws FileException, IOException, PageException, LogException {
        String fileName = "testGetFromFile.db";
        var itemStorage = ItemManager.fromFile(fileName);
        // 如果表中没有对应的文件，那么就抛出错误
        String failFileName = "error.db";
        try {
            IItemStorage iItemStorage1 = ItemManager.fromFile(failFileName);
        } catch (FileException f) {
            log.error("Exception Error :", f);
        }
    }

    /**
     * 测试能否新建文件并得到数据项管理器
     */
    public void testCreateFile() throws IOException, FileException, PageException, LogException {
        TransactionContext txContext = TransactionContext.empty();
        String fileName = "testCreateFile.db";
        byte[] metadata = new byte[1024];
        // 第一次执行的时候，表中没有数据，不会报错
        var iItemStorage = ItemManager.createFile(txContext, fileName, metadata);

        try {
            iItemStorage = ItemManager.createFile(txContext, fileName, metadata);
            fail("should get exception");
        } catch (Exception e) {
            log.error("Exception Error :", e);
        }
    }

    /**
     * 进行插入的测试
     */
    public void testInsert() throws FileException, IOException, PageException, UUIDException, PageCorruptedException, LogException {
        String fileName = "testInsert.db";
        IItemStorage iItemStorage = ItemManager.fromFile(fileName);
        byte[] bytes = new byte[]{1, 2, 3, 4};
        TransactionContext txContext = TransactionContext.empty();
        long uuid = iItemStorage.insertItem(txContext, bytes);

        long uuid2 = iItemStorage.insertItem(txContext, bytes);

        long uuid3 = iItemStorage.insertItem(txContext, bytes);

        assertArrayEquals(bytes, iItemStorage.queryItemByUuid(uuid));
    }

    /**
     * 对插入一个已分配UUID的测试
     */
    public void testInsertWithUUID() throws FileException, IOException, PageException, LogException {
        String fileName = "testInsertWithUUID.db";
        IItemStorage iItemStorage = ItemManager.fromFile(fileName);
        byte[] bytes = new byte[]{1, 2, 3, 4};
        long s = 1;
        int rnd = Math.abs(new Random().nextInt());
        long uuid = (s << 32) + rnd;
        TransactionContext txContext = TransactionContext.empty();
        // 第一次插入，表中没有该UUID，可以正常执行
        iItemStorage.insertItemWithUuid(bytes, uuid);
        try {
            assertArrayEquals(bytes, iItemStorage.queryItemByUuid(uuid));
        } catch (UUIDException | PageCorruptedException e) {
            e.printStackTrace();
        }

        // 第二次插入
        iItemStorage.insertItemWithUuid(bytes, uuid);

    }

    /**
     * 对插入大量数据进行测试
     */
    public void testManyInsert() throws FileException, IOException, PageException, UUIDException, PageCorruptedException, LogException {
        String fileName = "testInsertMany.db";
        IItemStorage iItemStorage = ItemManager.fromFile(fileName);
        byte[] bytes = new byte[]{1, 2, 3, 4};
        TransactionContext txContext = TransactionContext.empty();
        for (int i = 0; i < 500; i++) {
            long uuid = iItemStorage.insertItem(txContext, bytes);
            long uuid2 = iItemStorage.insertItem(txContext, bytes);
            long uuid3 = iItemStorage.insertItem(txContext, bytes);
            // 查询可以正常执行
            var item = iItemStorage.queryItemByUuid(uuid);
            assertArrayEquals(bytes, item);
            var item3 = iItemStorage.queryItemByUuid(uuid3);
            assertArrayEquals(bytes, item3);
        }
    }

    /**
     * 获取整个页的数据项进行测试
     */
    public void testQueryByPageID() throws FileException, IOException, PageException, PageCorruptedException, LogException {
        String fileName = "testQueryByPageID.db";
        IItemStorage iItemStorage = ItemManager.fromFile(fileName);
        byte[] bytes = new byte[]{1, 2, 3, 4};
        TransactionContext txContext = TransactionContext.empty();
        long uuid = iItemStorage.insertItem(txContext, bytes);

        byte[] bytes1 = new byte[]{2, 3, 4, 5};
        long uuid1 = iItemStorage.insertItem(txContext, bytes1);

        Comparator<byte[]> comparator = (o1, o2) -> {
            int length = Math.min(o1.length, o2.length);
            for (int i = 0; i < length; i++) {
                if (o2[i] > o1[i]) {
                    return -1;
                } else if (o2[i] < o1[i]) {
                    return 1;
                }
            }

            return Integer.compare(o1.length, o2.length);
        };

        List<byte[]> bs = new ArrayList<>();
        bs.add(bytes);
        bs.add(bytes1);
        bs.sort(comparator);
        // 获取pageID对应的数据项，在这里Mock是获取所有list中的数据
        var result = iItemStorage.listItemByPageId(1);
        result.sort(comparator);
        for (int i = 0; i < bs.size(); i++) {
            assertArrayEquals(bs.get(i), result.get(i));
        }
    }


    static class Insert implements Runnable {
        IItemStorage iItemStorage;
        TransactionContext txContext;

        public Insert(IItemStorage iItemStorage, TransactionContext txContext) {
            this.iItemStorage = iItemStorage;
            this.txContext = txContext;
        }

        @Override
        public void run() {
            var bytes = new byte[]{1, 2, 3, 4};
            try {
                for (int i = 0; i < 100; i++) {
                    long uuid = iItemStorage.insertItem(txContext, bytes);
                    assertArrayEquals(bytes, iItemStorage.queryItemByUuid(uuid));
                }
            } catch (Exception e) {
                e.printStackTrace();
                fail("Exception caught");
            }
        }
    }

    /**
     * 测试并发下插入是否有问题
     */
    public void testSynInsert() throws IOException, FileException, PageException, LogException {
        String fileName = "testInsert.db";
        IItemStorage iItemStorage = ItemManager.fromFile(fileName);
        byte[] bytes = new byte[]{1, 2, 3, 4};
        TransactionContext txContext = TransactionContext.empty();
        new Thread(new Insert(iItemStorage, txContext)).start();
        new Thread(new Insert(iItemStorage, txContext)).start();
        new Thread(new Insert(iItemStorage, txContext)).start();
        new Thread(new Insert(iItemStorage, txContext)).start();
    }


    /**
     * 对更新进行测试
     */
    public void testUpdate() throws FileException, IOException, PageException, UUIDException, PageCorruptedException, LogException {
        String fileName = "testUpdate.db";
        TransactionContext txContext = TransactionContext.empty();
        IItemStorage iItemStorage = ItemManager.fromFile(fileName);
        byte[] bytes = new byte[]{1, 2, 3, 4};
        long uuid = iItemStorage.insertItem(txContext, bytes);
        // 正常情况下进行修改
        byte[] result = new byte[]{2, 3, 4, 5};
        iItemStorage.updateItemByUuid(txContext, uuid, result);
        byte[] bs = iItemStorage.queryItemByUuid(uuid);
        assertArrayEquals(bs, result);
        // 修改一个不存在的UUID
        long s = 1;
        if (s == uuid) {
            s += 1;
        }
        try {
            iItemStorage.updateItemByUuid(txContext, s, result);
            fail("Should get exception");
        } catch (UUIDException e) {
            e.printStackTrace();
        }
    }

    /**
     * 测试修改和获取表头信息
     */
    public void testMeta() throws FileException, IOException, PageException, UUIDException, PageCorruptedException, LogException {
        String fileName = "testMeta.db";
        IItemStorage iItemStorage = ItemManager.fromFile(fileName);
        byte[] result = new byte[]{1, 2, 3, 4};
        TransactionContext txContext = TransactionContext.empty();
        iItemStorage.setMetadata(txContext, result);
        byte[] bs = iItemStorage.getMetadata();
        assertArrayEquals(result, bs);
    }

}
