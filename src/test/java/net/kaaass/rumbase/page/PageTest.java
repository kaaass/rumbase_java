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

    public void testGetData() {
        try {
            PageStorage pc = PageManager.fromFile("testFile");
            Page p0 = pc.get(0);
            Page p3 = pc.get(3);
            byte[] data0 = new byte[1024 * 4];
            byte[] data3 = new byte[1024 * 4];
            p0.getData().read(data0);
            p3.getData().read(data3);
            byte[] testData0 = new byte[1024 * 4];
            for (int j = 0; j < 1024 * 4; j++) {
                testData0[j] = (byte)5;
            }
            byte[] testData3 = new byte[1024 * 4];
            for (int j = 0; j < 1024 * 4; j++) {
                testData3[j] = (byte)8;
            }
            assertArrayEquals(testData0,data0);
            assertArrayEquals(testData3,data3);

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
}