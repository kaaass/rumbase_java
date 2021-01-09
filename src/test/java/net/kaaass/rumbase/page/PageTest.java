package net.kaaass.rumbase.page;

import junit.framework.TestCase;

import java.io.File;
import java.io.FileInputStream;

import static org.junit.Assert.assertArrayEquals;

/**
 * 页管理测试
 *
 * @author XuanLaoYee
 * @see net.kaaass.rumbase.page.Page
 */
public class PageTest extends TestCase {

    // TODO Mock的测试不应该和文件有关，本测试仅仅测试接口对上层的意义。而物理文件对上层不可见
    public void testGetData() {
        try {
            PageStorage pc = PageManager.fromFile("testFile");
            Page p0 = pc.get(0);
            Page p3 = pc.get(3);

            File file = new File("testFile");
            FileInputStream in0 = new FileInputStream(file);
            in0.skip((PageManager.FILE_HEAD_SIZE) * PageManager.PAGE_SIZE);
            byte[] data0 = new byte[PageManager.PAGE_SIZE];
            int readNumber1 = in0.read(data0);
            in0.close();
            assertArrayEquals(data0, p0.getDataBytes());
            assertEquals(PageManager.PAGE_SIZE, readNumber1);
            FileInputStream in3 = new FileInputStream(file);
            in3.skip((3 + PageManager.FILE_HEAD_SIZE) * PageManager.PAGE_SIZE);
            byte[] data3 = new byte[PageManager.PAGE_SIZE];
            int readNumber3 = in3.read(data3);
            assertArrayEquals(data3, p3.getDataBytes());
            assertEquals(PageManager.PAGE_SIZE, readNumber3);
            in3.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void testWriteData() {
        byte[] data = new byte[PageManager.PAGE_SIZE];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 120);
        }

        try {
            PageStorage pc = PageManager.fromFile("testFile");
            Page p0 = pc.get(0);
            //write之前需要先pin
            p0.pin();
            p0.writeData(data);
            //pin和unpin成对出现
            p0.unpin();
            assertArrayEquals(data, pc.get(0).getDataBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void testPatchData() {
        int offset = 99;
        byte[] data = new byte[PageManager.PAGE_SIZE - offset];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 120);
        }

        try {
            PageStorage pc = PageManager.fromFile("testFile");
            Page p0 = pc.get(0);
            byte[] originalData = p0.getDataBytes();
            p0.patchData(offset, data);
            byte[] newData = new byte[PageManager.PAGE_SIZE];
            System.arraycopy(originalData, 0, newData, 0, offset);
            System.arraycopy(data, 0, newData, offset, data.length);
            assertArrayEquals(newData, p0.getDataBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void testFlush() {
        byte[] data = new byte[PageManager.PAGE_SIZE];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 120);
        }

        try {
            PageStorage pc = PageManager.fromFile("testFile");
            Page p0 = pc.get(0);
            p0.writeData(data);
            p0.flush();
            p0 = pc.get(0);
            assertArrayEquals(data, p0.getDataBytes());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}