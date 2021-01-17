package net.kaaass.rumbase.page;

import net.kaaass.rumbase.FileUtil;
import net.kaaass.rumbase.page.exception.FileException;
import net.kaaass.rumbase.page.exception.PageException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Arrays;

import static org.junit.Assert.assertArrayEquals;

/**
 * 文件管理测试
 *
 * @author XuanLaoYee
 * @see net.kaaass.rumbase.page.PageStorage
 */
public class PageStorageTest {
    public static String filePath = FileUtil.TEST_PATH + "pageTest.db";

    @BeforeClass
    public static void createDataFolder() {
        FileUtil.prepare();
    }

    @AfterClass
    public static void clearDataFolder() {
        FileUtil.clear();
    }

    @Test
    public void testGet() throws IOException {
        PageStorage rps = null;
        try {
            rps = PageManager.fromFile(filePath);
        } catch (Exception e) {
            e.printStackTrace();
        }
        assert rps != null;
        for (int i = 0; i < 10; i++) {
            Page p = rps.get(i);
            p.pin();
        }
        Page p0 = rps.get(0);
        Page p1 = rps.get(1);
        p0.unpin();
        p1.unpin();
        Page p10 = rps.get(10);
        p10.pin();
        Page p11 = rps.get(11);
        p11.pin();

        File file = new File(filePath);
        try {
            FileInputStream in = new FileInputStream(file);
            byte[] dataFromFile = new byte[PageManager.PAGE_SIZE];
            byte[] dataFromPage = new byte[PageManager.PAGE_SIZE];
            in.skip((11 + PageManager.FILE_HEAD_SIZE) * PageManager.PAGE_SIZE);
            int readNumber = in.read(dataFromFile);
            p11.getData().read(dataFromPage);
            Assert.assertEquals(readNumber, PageManager.PAGE_SIZE);
            assertArrayEquals(dataFromPage, dataFromFile);
        } catch (Exception e) {
            throw e;
        } finally {
            p10.unpin();
            p11.unpin();
        }
    }

    @Test
    public void testWriteToFile() throws FileException, PageException {
        PageStorage storage = PageManager.fromFile(filePath);
        int[] testPage = new int[]{1, 3, 5, 7, 10, 11};
        // 测试每一页是否能正常读写
        for (var pageId : testPage) {
            // 准备标志数据
            byte[] data = new byte[PageManager.PAGE_SIZE];
            Arrays.fill(data, (byte) (0xF0 | pageId));
            // 获取页
            var page = storage.get(pageId);
            page.pin();
            // 写入页
            try {
                // 写数据
                page.patchData(0, data);
                page.flush();
            } finally {
                page.unpin();
            }
            // 检查页数据
            var tempStorage = PageManager.fromFile(filePath);
            var pageData = tempStorage.get(pageId).getDataBytes();
            assertArrayEquals(data, pageData);
        }
    }

    @Test
    public void testFlush() throws PageException {
        PageStorage rps = null;
        try {
            rps = PageManager.fromFile(filePath);
        } catch (Exception e) {
            e.printStackTrace();
        }
        assert rps != null;
        Page p0 = rps.get(0);
        Page p3 = rps.get(3);
        Page p4 = rps.get(4);
        p0.pin();
        p3.pin();
        p4.pin();
        byte[] data0 = new byte[PageManager.PAGE_SIZE];
        byte[] data3 = new byte[PageManager.PAGE_SIZE];
        byte[] data4 = new byte[PageManager.PAGE_SIZE];
        byte[] dataFromFile0 = new byte[PageManager.PAGE_SIZE];
        byte[] dataFromFile3 = new byte[PageManager.PAGE_SIZE];
        byte[] dataFromFile4 = new byte[PageManager.PAGE_SIZE];
        for (int i = 0; i < PageManager.PAGE_SIZE; i++) {
            data0[i] = (byte) 0xf0;
            data3[i] = (byte) 0xf3;
            data4[i] = (byte) 0xf4;
        }
        // 打印下之前的数据，检查之前写入是否正确
        try {
            p0.writeData(data0);
            p3.writeData(data3);
            p4.writeData(data4);
            rps.flush();
        } catch (Exception e) {
            throw e;
        } finally {
            p0.unpin();
            p3.unpin();
            p4.unpin();
        }
        File file = new File(filePath);
        try {
            FileInputStream in = new FileInputStream(file);
            in.skip((PageManager.FILE_HEAD_SIZE) * PageManager.PAGE_SIZE);
            in.read(dataFromFile0);
            in.close();
            in = new FileInputStream(file);
            in.skip((3 + PageManager.FILE_HEAD_SIZE) * PageManager.PAGE_SIZE);
            in.read(dataFromFile3);
            in.close();
            in = new FileInputStream(file);
            in.skip((4 + PageManager.FILE_HEAD_SIZE) * PageManager.PAGE_SIZE);
            in.read(dataFromFile4);
            in.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            p0.getData().read(data0);
            p3.getData().read(data3);
            p4.getData().read(data4);
        } catch (Exception e) {
            e.printStackTrace();
        }
        assertArrayEquals(data0, dataFromFile0);
        assertArrayEquals(data3, dataFromFile3);
        assertArrayEquals(data4, dataFromFile4);
    }

    @Test
    public void testFlushAll() throws FileException, PageException {
        PageStorage storage = PageManager.fromFile(filePath);
        int[] testPage = new int[]{1, 3, 5, 7, 10, 11};
        // 测试每一页是否能正常读写
        for (var pageId : testPage) {
            // 准备标志数据
            byte[] data = new byte[PageManager.PAGE_SIZE];
            Arrays.fill(data, (byte) (0xF0 | pageId));
            // 获取页
            var page = storage.get(pageId);
            page.pin();
            // 写入页
            try {
                // 写数据
                page.patchData(0, data);
            } finally {
                page.unpin();
            }
        }
        //将内存中的所有页都写回
        PageManager.flush();
        for (var pageId : testPage) {
            byte[] data = new byte[PageManager.PAGE_SIZE];
            Arrays.fill(data, (byte) (0xF0 | pageId));
            // 获取页
            var page = storage.get(pageId);
            page.pin();
            // 写入页
            try {
                // 检查页数据
                var tempStorage = PageManager.fromFile(filePath);
                var pageData = tempStorage.get(pageId).getDataBytes();
                assertArrayEquals(data, pageData);
            } finally {
                page.unpin();
            }

        }
    }
}