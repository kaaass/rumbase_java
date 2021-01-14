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
    public static String filePath = "src/test/test";
    public void testPatchData() {
        int offset = 99;
        byte[] data = new byte[PageManager.PAGE_SIZE - offset];
        for (int i = 0; i < data.length; i++) {
            data[i] = (byte) (i % 120);
        }

        try {
            PageStorage pc = PageManager.fromFile(filePath);
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