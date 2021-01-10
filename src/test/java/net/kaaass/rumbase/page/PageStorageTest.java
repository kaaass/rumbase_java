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
    public void testGet() {
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
}