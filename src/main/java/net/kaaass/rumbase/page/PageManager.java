package net.kaaass.rumbase.page;

import net.kaaass.rumbase.page.exception.FileException;
import net.kaaass.rumbase.page.mock.MockPageStorage;

/**
 * @author XuanLaoYee
 */
public class PageManager {
    public static int PAGE_SIZE = 1024 * 4; // 页面大小是4KB
    public static long FILE_HEAD_SIZE = 5; // 文件头留5页
    public static int PAGE_NUM = 50;
    public static int BYTE_BUFFER_SIZE = 1024 * 4 * PAGE_NUM;

    /**
     * TODO 文档
     *
     * @param filepath
     * @return
     * @throws FileException
     */
    public static PageStorage fromFile(String filepath) throws FileException {
        return new MockPageStorage(filepath);
    }
}
