package net.kaaass.rumbase.page;

import junit.framework.TestCase;

import java.io.File;
import java.io.FileInputStream;

import static org.junit.Assert.assertArrayEquals;

/**
 * 页管理测试
 *
 * @see net.kaaass.rumbase.page.Page
 * @author XuanLaoYee
 */
public class PageTest extends TestCase {

    public void testGetData() {
        try{
            PageCache pc = PageManager.fromFile("testFile");
            Page p0 = pc.get(0);
            Page p3 = pc.get(3);

            File file = new File("testFile");
            FileInputStream in0 = new FileInputStream(file);
            in0.skip((PageManager.FILE_HEAD_SIZE) * PageManager.PAGE_SIZE);
            byte[] data0 = new byte[PageManager.PAGE_SIZE];
            int readNumber1 = in0.read(data0);
            in0.close();
            assertArrayEquals(data0,p0.getData());
            assertEquals(PageManager.PAGE_SIZE,readNumber1);
            FileInputStream in3 = new FileInputStream(file);
            in3.skip((3 + PageManager.FILE_HEAD_SIZE) * PageManager.PAGE_SIZE);
            byte[] data3 = new byte[PageManager.PAGE_SIZE];
            int readNumber3 = in3.read(data3);
            assertArrayEquals(data3,p3.getData());
            assertEquals(PageManager.PAGE_SIZE,readNumber3);
            in3.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void testWriteData() {
        byte[] data = new byte[PageManager.PAGE_SIZE];
        for(int i = 0; i < data.length; i++) {
            data[i] = (byte)(i%120);
        }

        try{
            PageCache pc = PageManager.fromFile("testFile");
            Page p0 = pc.get(0);
            //write之前需要先pin
            p0.pin();
            p0.writeData(data);
            //pin和unpin成对出现
            p0.unpin();
            assertEquals(data,p0.getData());
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void testPatchData() {
        int offset = 99;
        byte[] data = new byte[PageManager.PAGE_SIZE - offset];
        for(int i = 0; i < data.length; i++) {
            data[i] = (byte)(i%120);
        }

        try{
            PageCache pc = PageManager.fromFile("testFile");
            Page p0 = pc.get(0);
            byte[] originalData = p0.getData();
            p0.patchData(offset,data);
            byte[] newData = new byte[PageManager.PAGE_SIZE];
            System.arraycopy(originalData,0,newData,0,offset);
            System.arraycopy(data,0,newData,offset,data.length);
            assertArrayEquals(newData,p0.getData());
        }catch (Exception e) {
            e.printStackTrace();
        }
    }

    public void testFlush() {
        byte[] data = new byte[PageManager.PAGE_SIZE];
        for(int i = 0; i < data.length; i++) {
            data[i] = (byte)(i%120);
        }

        try{
            PageCache pc = PageManager.fromFile("testFile");
            Page p0 = pc.get(0);
            p0.writeData(data);
            p0.flush();
            p0 = pc.get(0);
            assertArrayEquals(data,p0.getData());
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
}