package net.kaaass.rumbase.dataitem;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.dataitem.exception.FileExistException;
import net.kaaass.rumbase.dataitem.exception.UUIDException;

import java.util.ArrayList;
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
    public void testGetFromFile() throws FileExistException {
        String fileName = "testGetFromFile.db";
        var itemStorage = ItemManager.fromFile(fileName);

        // 如果表中没有对应的文件，那么就抛出错误
        String failFileName = "error.db";
        try {
            IItemStorage iItemStorage1 = ItemManager.fromFile(failFileName);

        } catch (FileExistException f) {
            log.error("Exception Error :", f);
        }
    }

    /**
     * 测试能否新建文件并得到数据项管理器
     */
    public void testCreateFile() {
        String fileName = "testCreateFile.db";
        byte[] metadata = new byte[1024];
        // 第一次执行的时候，表中没有数据，不会报错
        try {
            var iItemStorage = ItemManager.createFile(fileName, metadata);
        } catch (Exception e) {
            e.printStackTrace();
        }

        try {
            IItemStorage iItemStorage = ItemManager.createFile(fileName, metadata);
        } catch (FileExistException e) {
            log.error("Exception Error :", e);
        }
    }

    /**
     * 进行插入的测试
     */
    public void testInsert() throws FileExistException {
        String fileName = "testInsert.db";
        IItemStorage iItemStorage = ItemManager.fromFile(fileName);
        byte[] bytes = new byte[]{1, 2, 3, 4};
        long uuid = iItemStorage.insertItem(bytes);
//        try {
            assertEquals(bytes, iItemStorage.queryItemByUUID(uuid));
//        } catch (UUIDException e) {
//            e.printStackTrace();
//        }
    }

    /**
     * 对插入一个已分配UUID的测试
     */
    public void testInsertWithUUID() throws FileExistException {
        String fileName = "testInsertWithUUID.db";
        IItemStorage iItemStorage = ItemManager.fromFile(fileName);
        byte[] bytes = new byte[]{1, 2, 3, 4};
        long uuid = 50;
        // 第一次插入，表中没有该UUID，可以正常执行
        try {
            iItemStorage.insertItemWithUUID(bytes, uuid);
            assertEquals(bytes, iItemStorage.queryItemByUUID(uuid));
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 第二次插入，应该报错，因为已经存在该UUID了
        try {
            iItemStorage.insertItemWithUUID(bytes, uuid);
        } catch (UUIDException e) {
            log.error("Exception Error :", e);
        }
    }

    /**
     * 对查询进行测试
     */
    public void testQuery() throws FileExistException {
        String fileName = "testQuery.db";
        IItemStorage iItemStorage = ItemManager.fromFile(fileName);
        byte[] bytes = new byte[]{1, 2, 3, 4};
        long uuid = iItemStorage.insertItem(bytes);
        // 查询可以正常执行
//        try {
            assertEquals(bytes, iItemStorage.queryItemByUUID(uuid));
//        } catch (UUIDException e) {
//            e.printStackTrace();
//        }
        // 找一个跟UUID不同的，也就是说不在数据库中的UUID进行查询
        long s = 1;
        if (s == uuid) {
            s += 1;
        }
//        try {
            var b = iItemStorage.queryItemByUUID(s);
//        } catch (UUIDException e) {
//            log.error("Exception Error :", e);
//        }
    }

    /**
     * 获取整个页的数据项进行测试
     */
    public void testQueryByPageID() throws FileExistException {
        String fileName = "testQueryByPageID.db";
        IItemStorage iItemStorage = ItemManager.fromFile(fileName);
        byte[] bytes = new byte[]{1, 2, 3, 4};
        long uuid = iItemStorage.insertItem(bytes);

        byte[] bytes1 = new byte[]{2, 3, 4, 5};
        long uuid1 = iItemStorage.insertItem(bytes1);

        try {
            List<byte[]> bs = new ArrayList<>();
            bs.add(bytes1);
            bs.add(bytes);
            // 获取pageID对应的数据项，在这里Mock是获取所有list中的数据
            var result = iItemStorage.listItemByPageId(0);
            assertEquals(bs, result);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 对更新进行测试
     */
    public void testUpdate() throws FileExistException {
        String fileName = "testUpdate.db";
        IItemStorage iItemStorage = ItemManager.fromFile(fileName);
        byte[] bytes = new byte[]{1, 2, 3, 4};
        long uuid = iItemStorage.insertItem(bytes);
        // 正常情况下进行修改
        byte[] result = new byte[]{2, 3, 4, 5};
        try {
            iItemStorage.updateItemByUUID(uuid, result);
            byte[] bs = iItemStorage.queryItemByUUID(uuid);
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
            iItemStorage.updateItemByUUID(s, result);
        } catch (UUIDException e) {
            e.printStackTrace();
        }
    }

    /**
     * 测试修改和获取表头信息
     */
    public void testMeta() throws FileExistException {
        String fileName = "testMeta.db";
        IItemStorage iItemStorage = ItemManager.fromFile(fileName);
        byte[] result = new byte[]{1, 2, 3, 4};

        iItemStorage.setMetadata(result);
        byte[] bs = iItemStorage.getMetadata();
        assertEquals(result, bs);

    }
}
