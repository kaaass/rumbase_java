package net.kaaass.rumbase.page;

import net.kaaass.rumbase.page.exception.BufferException;
import net.kaaass.rumbase.page.exception.FileException;

import java.io.*;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author 11158
 */
public class RumPageStorage implements PageStorage {
    public RumPageStorage(String filepath) throws FileException {
        this.filepath = filepath;
        pageMap = new HashMap<>();
    }

    @Override
    public Page get(long pageId) {
        File file = new File(filepath);
        //文件不存在时创建新文件
        if (!file.exists()) {
            try {
                file.createNewFile();
                FileOutputStream out = new FileOutputStream(file);
                out.write(new byte[PageManager.PAGE_SIZE * 10]);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        //文件会预留5页作为文件头
        try {
            FileInputStream in = new FileInputStream(file);
            byte[] data = new byte[PageManager.PAGE_SIZE];
            //当文件存储容量不够时追加
            try {
                while (in.available() < (pageId + PageManager.FILE_HEAD_SIZE) * PageManager.PAGE_SIZE) {
                    FileWriter fw = new FileWriter(file, true);
                    fw.append(Arrays.toString(new byte[PageManager.PAGE_SIZE * (int) (in.available() / PageManager.PAGE_SIZE)]));
                }
                in.skip((pageId + PageManager.FILE_HEAD_SIZE) * PageManager.PAGE_SIZE);
                int readNumber = in.read(data);
                if (readNumber < PageManager.PAGE_SIZE) {
                    throw new FileException(4);
                }
            } catch (Exception e) {
                throw new FileException(4);
            }

            Integer tmpId = (int) pageId;
            if (pageMap.containsKey(tmpId)) {
                return pageMap.get(tmpId);
            }
            int offset = -1;
            while (offset < 0) {
                synchronized (this) {//并非区间锁，而是将整个内存全部锁住
                    try {
                        offset = RumBuffer.getInstance().getFreeOffset();
                        RumBuffer.getInstance().put(offset, data);
                    } catch (BufferException e) {
                        //下面的这个换出算法没有考虑到在此过程中其他进程再次pin()的情况
                        RumPage p = Replacer.getInstance().victim();
                        if (p.dirty()) {
                            p.flush();
                        }
                        RumBuffer.getInstance().free(p.offset);
                    }
                }
            }
            RumPage page = new RumPage(RumBuffer.getInstance().buffer(), pageId, this.filepath, offset);
            Replacer.getInstance().insert(page);
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
