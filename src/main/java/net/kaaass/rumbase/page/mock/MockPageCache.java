package net.kaaass.rumbase.page.mock;

import net.kaaass.rumbase.page.Page;
import net.kaaass.rumbase.page.PageCache;
import net.kaaass.rumbase.page.PageManager;
import net.kaaass.rumbase.page.exception.FileExeception;

import java.io.*;
import java.util.HashMap;
import java.util.Map;


/**
 * @author XuanLaoYee
 */
public class MockPageCache implements PageCache {


    public MockPageCache(String filepath) throws FileExeception {
        this.filepath = filepath;
        File file = new File(filepath);
        pageMap = new HashMap<>();
        if (!file.exists()) {
            try {
                file.createNewFile();
                OutputStream os = null;
                try {
                    os = new FileOutputStream(file, true);
                    byte[] data = new byte[PageManager.PAGE_SIZE];
                    for (int i = 0; i < 10; i++) {
                        os.write(data, 0, data.length);
                        os.flush();
                    }
                } catch (Exception e) {
                    throw new FileExeception(2);
                }
            } catch (Exception e) {
                throw new FileExeception(1);
            }
        }
    }

    @Override
    public Page get(long pageId) throws FileExeception {
        File file = new File(filepath);
        //文件会预留5页作为文件头
        try {
            FileInputStream in = new FileInputStream(file);
            byte[] data = new byte[PageManager.PAGE_SIZE];
            try{
                in.skip((pageId + PageManager.FILE_HEAD_SIZE) * PageManager.PAGE_SIZE);
                int readNumber = in.read(data);
                if(readNumber<PageManager.PAGE_SIZE){
                    throw new FileExeception(4);
                }
            }catch(Exception e){
                throw new FileExeception(4);
            }
            Integer tmpId = (int) pageId;
            if(pageMap.containsKey(tmpId)){
                return pageMap.get(tmpId);
            }
            Page page = new MockPage(data, pageId, this.filepath);
            pageMap.put(tmpId, page);
            return page;
        } catch (Exception e) {
            throw new FileExeception(3);
        }
    }

    @Override
    public void flush() {
        //TODO
    }
    private Map<Integer,Page> pageMap;

    private String filepath;
}
