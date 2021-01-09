package net.kaaass.rumbase.page;

import junit.framework.TestCase;

import java.io.File;
import java.io.FileInputStream;

import static org.junit.Assert.assertArrayEquals;

public class PageCacheTest extends TestCase {
    public void testGet(){
        try{
            PageCache pc = PageManager.fromFile("testFile");
            Page p0 = pc.get(0);
            Page p3 = pc.get(3);

            File file = new File("testFile");
            FileInputStream in = new FileInputStream(file);
            in.skip((PageManager.FILE_HEAD_SIZE) * PageManager.PAGE_SIZE);
            byte[] data0 = new byte[PageManager.PAGE_SIZE];
            in.read(data0);
            in.close();
            assertArrayEquals(data0,p0.getData());
            in = new FileInputStream(file);
            in.skip((3 + PageManager.FILE_HEAD_SIZE) * PageManager.PAGE_SIZE);
            byte[] data3 = new byte[PageManager.PAGE_SIZE];
            in.skip((PageManager.FILE_HEAD_SIZE) * PageManager.PAGE_SIZE);
            in.read(data3);
            assertArrayEquals(data3,p3.getData());


        }catch(Exception e){
            e.printStackTrace();
        }

    }
}