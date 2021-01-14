package net.kaaass.rumbase.page;

import junit.framework.TestCase;

import java.io.File;
import java.io.FileInputStream;

import static org.junit.Assert.assertArrayEquals;

/**
 * 文件管理测试
 *
 * @author XuanLaoYee
 * @see net.kaaass.rumbase.page.PageStorage
 */
public class PageStorageTest extends TestCase {
    public static String filePath = "src/test/test";

    public void testGet() {
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
            assertEquals(readNumber, PageManager.PAGE_SIZE);
            assertArrayEquals(dataFromPage, dataFromFile);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            p10.unpin();
            p11.unpin();
        }
    }

    public void testFlush() {
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
        byte[] data0 = new byte[PageManager.PAGE_SIZE];
        byte[] data3 = new byte[PageManager.PAGE_SIZE];
        byte[] data4 = new byte[PageManager.PAGE_SIZE];
        byte[] dataFromFile0 = new byte[PageManager.PAGE_SIZE];
        byte[] dataFromFile3 = new byte[PageManager.PAGE_SIZE];
        byte[] dataFromFile4 = new byte[PageManager.PAGE_SIZE];
        for (int i = 0; i < PageManager.PAGE_SIZE; i++) {
            data0[i] = (byte) (i);
            data3[i] = (byte) (i);
            data4[i] = (byte) (i);
        }

        p0.pin();
        p3.pin();
        p4.pin();
        try {
            p0.writeData(data0);
            p3.writeData(data3);
            p4.writeData(data4);
//            p0.flush();
//            p3.flush();
//            p4.flush();
            rps.flush();
        } catch (Exception e) {
            e.printStackTrace();
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
        try{
            p0.getData().read(data0);
            p3.getData().read(data3);
            p4.getData().read(data4);
        }catch (Exception e) {
            e.printStackTrace();
        }
        assertArrayEquals(data0, dataFromFile0);
        assertArrayEquals(data3, dataFromFile3);
        assertArrayEquals(data4, dataFromFile4);
    }

}