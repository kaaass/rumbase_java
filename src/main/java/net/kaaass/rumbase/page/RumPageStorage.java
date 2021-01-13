package net.kaaass.rumbase.page;

import net.kaaass.rumbase.page.exception.BufferException;
import net.kaaass.rumbase.page.exception.FileException;
import net.kaaass.rumbase.page.mock.MockPage;

import java.io.File;
import java.io.FileInputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

public class RumPageStorage implements PageStorage {
    public RumPageStorage(String filepath) throws FileException {
        this.filepath = filepath;
        pageMap = new HashMap<>();
    }

    @Override
    public Page get(long pageId) {
        File file = new File(filepath);
        //文件会预留5页作为文件头
        try {
            FileInputStream in = new FileInputStream(file);
            byte[] data = new byte[PageManager.PAGE_SIZE];
            try {
                in.skip((pageId + PageManager.FILE_HEAD_SIZE) * PageManager.PAGE_SIZE);
                int readNumber = in.read(data);
                if (readNumber < PageManager.PAGE_SIZE) {
                    throw new FileException(4);
                }
            } catch (Exception e) {
                throw new FileException(4);
            }
            Integer tmpId = (int) pageId;
            if(pageMap.containsKey(tmpId)){
                return pageMap.get(tmpId);
            }
            int offset = -1;
            while(offset<0){
                synchronized (this){//并非区间锁，而是将整个内存全部锁住
                    try{
                        offset = RumBuffer.getInstance().getFreeOffset();
                        RumBuffer.getInstance().put(offset,data);
                    }catch (BufferException e) {
                        //下面的这个换出算法没有考虑到在此过程中其他进程再次pin()的情况
                        RumPage p = Replacer.getInstance().victim();
                        if(p.dirty()){
                            p.flush();
                        }
                        RumBuffer.getInstance().free(p.offset);
                    }
                }
            }
            Page page = new RumPage(RumBuffer.getInstance().buffer(), pageId, this.filepath,offset);
            pageMap.put(tmpId, page);
            return page;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void flush() {
        Set<Map.Entry<Integer, Page>> entrySet = this.pageMap.entrySet();
        for (Map.Entry<Integer, Page> entry : entrySet) {
            try {
                entry.getValue().flush();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    private final Map<Integer, Page> pageMap;
    private final String filepath;
}
