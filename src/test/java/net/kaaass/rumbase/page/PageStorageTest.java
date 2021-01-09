package net.kaaass.rumbase.page;

import junit.framework.TestCase;

import java.io.File;
import java.io.FileInputStream;

import static org.junit.Assert.assertArrayEquals;

/**
 * TODO 文档
 * TODO Mock的测试不应该和文件有关，本测试仅仅测试接口对上层的意义。而物理文件对上层不可见
 */
public class PageStorageTest extends TestCase {
    public void testGet() {
        try {
            PageStorage pc = PageManager.fromFile("testFile");
            Page p0 = pc.get(0);
            p0 = pc.get(0);
            Page p3 = pc.get(3);

            File file = new File("testFile");
            FileInputStream in = new FileInputStream(file);
            in.skip((PageManager.FILE_HEAD_SIZE) * PageManager.PAGE_SIZE);
            byte[] data0 = new byte[PageManager.PAGE_SIZE];
            in.read(data0);
            in.close();
            assertArrayEquals(data0, p0.getDataBytes());
            in = new FileInputStream(file);
            in.skip((3 + PageManager.FILE_HEAD_SIZE) * PageManager.PAGE_SIZE);
            byte[] data3 = new byte[PageManager.PAGE_SIZE];
            in.skip((PageManager.FILE_HEAD_SIZE) * PageManager.PAGE_SIZE);
            in.read(data3);
            assertArrayEquals(data3, p3.getDataBytes());


        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}