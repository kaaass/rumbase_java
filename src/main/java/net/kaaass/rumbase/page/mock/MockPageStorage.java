package net.kaaass.rumbase.page.mock;

import net.kaaass.rumbase.page.Page;
import net.kaaass.rumbase.page.PageManager;
import net.kaaass.rumbase.page.PageStorage;
import net.kaaass.rumbase.page.exception.FileException;

import java.util.HashMap;
import java.util.Map;

/**
 * @author XuanLaoYee
 */
public class MockPageStorage implements PageStorage {


    public MockPageStorage(String filepath) throws FileException {
        this.filepath = filepath;
        pageMap = new HashMap<>();
        this.fakeFile = new byte[1024 * 4 * 20];
        for (int i = 0; i < 20; i++) {
            for (int j = 0; j < 1024 * 4; j++) {
                this.fakeFile[1024 * 4 * i + j] = (byte) i;
            }
        }
    }

    @Override
    public Page get(long pageId) {
        //文件会预留5页作为文件头
        try {
            byte[] data = new byte[PageManager.PAGE_SIZE];
            System.arraycopy(fakeFile, (int) (pageId + PageManager.FILE_HEAD_SIZE) * PageManager.PAGE_SIZE, data, 0, data.length);
            Integer tmpId = (int) pageId;
            if (pageMap.containsKey(tmpId)) {
                return pageMap.get(tmpId);
            }
            Page page = new MockPage(data, pageId, this.filepath);
            pageMap.put(tmpId, page);
            return page;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public void flush() {
    }

    private Map<Integer, Page> pageMap;

    private String filepath;
    private byte[] fakeFile;
}
