package net.kaaass.rumbase.page;

import junit.framework.TestCase;
import net.kaaass.rumbase.page.exception.FileException;
import net.kaaass.rumbase.page.exception.PageException;
import org.junit.Assert;

import java.io.File;
import java.io.FileInputStream;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;

/**
 * 页管理测试
 *
 * @author XuanLaoYee
 * @see net.kaaass.rumbase.page.Page
 */
public class PageTest extends TestCase {
    public static String filePath = "build/pageTest.db";
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

    public void testPatchOffset() throws FileException, PageException {
        var storage = PageManager.fromFile(filePath);
        var rand = new Random();
        var page = storage.get(2);
        page.pin();
        try {
            for (int i = 0; i < 50; i++) {
                // 随机决定开始、结束
                var st = rand.nextInt(4000);
                var ed = st + rand.nextInt(2000);
                if (ed > 4096) {
                    ed = 4096;
                }
                // 生成相关数据
                byte[] data = new byte[ed - st];
                Arrays.fill(data, (byte) st);
                // 写入
                page.patchData(st, data);
                // 检查写入效果
                var pageData = page.getDataBytes();
                for (int j = st; j < ed; j++) {
                    assertEquals((byte) st, pageData[j]);
                }
            }
        } finally {
            page.unpin();
        }
    }
}