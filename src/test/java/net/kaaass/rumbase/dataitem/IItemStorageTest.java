package net.kaaass.rumbase.dataitem;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.dataitem.exception.UUIDException;
import net.kaaass.rumbase.page.exception.FileException;
import net.kaaass.rumbase.page.exception.PageException;
import net.kaaass.rumbase.transaction.TransactionContext;
import net.kaaass.rumbase.transaction.mock.MockTransactionContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

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
    public void testGetFromFile() throws FileException, IOException, PageException {
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
    public void testCreateFile() {
        TransactionContext txContext = new MockTransactionContext();
        String fileName = "testCreateFile.db";
        byte[] metadata = new byte[1024];
        // 第一次执行的时候，表中没有数据，不会报错
        try {
            var iItemStorage = ItemManager.createFile(txContext,fileName, metadata);
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            IItemStorage iItemStorage = ItemManager.createFile(txContext,fileName, metadata);
        } catch (Exception e) {
            log.error("Exception Error :", e);
        }
    }

    /**
     * 进行插入的测试
     */
    public void testInsert() throws FileException, IOException, PageException {
        String fileName = "testInsert.db";
        IItemStorage iItemStorage = ItemManager.fromFile(fileName);
        byte[] bytes = new byte[]{1, 2, 3, 4};
        TransactionContext txContext = new MockTransactionContext();
        long uuid = iItemStorage.insertItem(txContext,bytes);
        try {
            assertEquals(bytes, iItemStorage.queryItemByUuid(uuid));
        } catch (UUIDException e) {
            e.printStackTrace();
        }
    }

    /**
     * 对插入一个已分配UUID的测试
     */
    public void testInsertWithUUID() throws FileException, IOException, PageException {
        String fileName = "testInsertWithUUID.db";
        IItemStorage iItemStorage = ItemManager.fromFile(fileName);
        byte[] bytes = new byte[]{1, 2, 3, 4};
        long uuid = 50;
        TransactionContext txContext = new MockTransactionContext();
        // 第一次插入，表中没有该UUID，可以正常执行
        iItemStorage.insertItemWithUuid(txContext,bytes, uuid);
        try {
            assertEquals(bytes, iItemStorage.queryItemByUuid(uuid));
        } catch (UUIDException e) {
            e.printStackTrace();
        }

        // 第二次插入
        iItemStorage.insertItemWithUuid(txContext,bytes, uuid);

    }

    /**
     * 对查询进行测试
     */
    public void testQuery() throws FileException, IOException, PageException {
        String fileName = "testQuery.db";
        IItemStorage iItemStorage = ItemManager.fromFile(fileName);
        byte[] bytes = new byte[]{1, 2, 3, 4};
        TransactionContext txContext = new MockTransactionContext();
        long uuid = iItemStorage.insertItem(txContext,bytes);
        // 查询可以正常执行
        try {
            assertEquals(bytes, iItemStorage.queryItemByUuid(uuid));
        } catch (UUIDException e) {
            e.printStackTrace();
        }
        // 找一个跟UUID不同的，也就是说不在数据库中的UUID进行查询
        long s = 1;
        if (s == uuid) {
            s += 1;
        }
        try {
            var b = iItemStorage.queryItemByUuid(s);
        } catch (UUIDException e) {
            log.error("Exception Error :", e);
        }
    }

    /**
     * 获取整个页的数据项进行测试
     */
    public void testQueryByPageID() throws FileException, IOException, PageException {
        String fileName = "testQueryByPageID.db";
        IItemStorage iItemStorage = ItemManager.fromFile(fileName);
        byte[] bytes = new byte[]{1, 2, 3, 4};
        TransactionContext txContext = new MockTransactionContext();
        long uuid = iItemStorage.insertItem(txContext,bytes);

        byte[] bytes1 = new byte[]{2, 3, 4, 5};
        long uuid1 = iItemStorage.insertItem(txContext,bytes1);

        Comparator<byte[]> comparator = new Comparator<byte[]>() {
            @Override
            public int compare(byte[] o1, byte[] o2) {
                int length = Math.min(o1.length, o2.length);
                for (int i = 0; i < length; i++) {
                    if (o2[i] > o1[i]) {
                        return -1;
                    } else if (o2[i] < o1[i]) {
                        return 1;
                    }
                }

                return Integer.compare(o1.length, o2.length);
            }
        };

        try {
            List<byte[]> bs = new ArrayList<>();
            bs.add(bytes);
            bs.add(bytes1);
            bs.sort(comparator);
            // 获取pageID对应的数据项，在这里Mock是获取所有list中的数据
            var result = iItemStorage.listItemByPageId(0);
            result.sort(comparator);
            assertEquals(bs, result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 对更新进行测试
     */
    public void testUpdate() throws FileException, IOException, PageException {
        String fileName = "testUpdate.db";
        TransactionContext txContext = new MockTransactionContext();
        IItemStorage iItemStorage = ItemManager.fromFile(fileName);
        byte[] bytes = new byte[]{1, 2, 3, 4};
        long uuid = iItemStorage.insertItem(txContext,bytes);
        // 正常情况下进行修改
        byte[] result = new byte[]{2, 3, 4, 5};
        try {
            iItemStorage.updateItemByUuid(txContext,uuid, result);
            byte[] bs = iItemStorage.queryItemByUuid(uuid);
            assertEquals(bs, result);
        } catch (UUIDException e) {
            e.printStackTrace();
        }
        // 修改一个不存在的UUID
        long s = 1;
        if (s == uuid) {
            s += 1;
        }
        try {
            iItemStorage.updateItemByUuid(txContext,s, result);
        } catch (UUIDException e) {
            e.printStackTrace();
        }
    }

    /**
     * 测试修改和获取表头信息
     */
    public void testMeta() throws FileException, IOException, PageException {
        String fileName = "testMeta.db";
        IItemStorage iItemStorage = ItemManager.fromFile(fileName);
        byte[] result = new byte[]{1, 2, 3, 4};
        TransactionContext txContext = new MockTransactionContext();
        iItemStorage.setMetadata(txContext,result);
        byte[] bs = iItemStorage.getMetadata();
        assertEquals(result, bs);

    }

}
