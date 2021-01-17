package net.kaaass.rumbase.page;

import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.page.exception.BufferException;
import net.kaaass.rumbase.page.exception.FileException;

import java.io.*;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author 11158
 */
@Slf4j
public class RumPageStorage implements PageStorage {
    public RumPageStorage(String filepath) {
        this.filepath = filepath;
        pageMap = new ConcurrentHashMap<>();
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
                log.error("创建文件失败", e);
                System.exit(1);
            }
        }
        //文件会预留5页作为文件头
        try {
            FileInputStream in = new FileInputStream(file);
            byte[] data = new byte[PageManager.PAGE_SIZE];
            try {
                //当文件存储容量不够时追加
                while (in.available() < (pageId + 1 + PageManager.FILE_HEAD_SIZE) * PageManager.PAGE_SIZE) {
                    FileWriter fw = new FileWriter(file, true);
                    char[] blank = new char[PageManager.PAGE_SIZE * (in.available() / PageManager.PAGE_SIZE)];
                    Arrays.fill(blank, (char) 0);
                    fw.write(blank);
                    fw.close();
                }
                in.skip((pageId + PageManager.FILE_HEAD_SIZE) * PageManager.PAGE_SIZE);
                int readNumber = in.read(data);
                if (readNumber < PageManager.PAGE_SIZE) {
                    throw new FileException(4);
                }
            } catch (Exception e) {
                throw new FileException(4);
            }

            Long tmpId = pageId;
            if (pageMap.containsKey(tmpId)) {
                return pageMap.get(tmpId);
            }
            int offset = -1;
            while (offset < 0) {
                //并非区间锁，而是将整个内存全部锁住
                synchronized (RumBuffer.getInstance()) {
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
                        this.pageMap.remove(p.pageId());
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
        var entrySet = this.pageMap.entrySet();
        for (var entry : entrySet) {
            var page = entry.getValue();
            try {
                if (page instanceof RumPage && ((RumPage) page).dirty()) {
                    page.flush();
                }
            } catch (Exception e) {
                log.warn("回写页面 {} 发生异常", entry.getKey());
            }
        }
    }

    private final Map<Long, Page> pageMap;
    private final String filepath;
}
