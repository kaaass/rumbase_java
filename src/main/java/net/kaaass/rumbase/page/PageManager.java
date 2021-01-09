package net.kaaass.rumbase.page;

import net.kaaass.rumbase.page.exception.FileExeception;
import net.kaaass.rumbase.page.mock.MockPageCache;

public class PageManager {
    public static int PAGE_SIZE = 1024 * 4;//页面大小是4KB
    public static long FILE_HEAD_SIZE = 5;//文件头留5页
    public static int PAGE_NUM = 50;
    public static int BYTE_BUFFER_SIZE = 1024 * 4 * PAGE_NUM;
    public static PageCache fromFile(String filepath) throws FileExeception {
        return new MockPageCache(filepath);
    }
}
