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
     * 取数据库文件生成文件管理的对象
     *
     * @param filepath 每个表文件路径
     * @return
     * @throws FileException 若文件不存在则创建，创建过程中出现错误会抛出错误
     */
    public static PageStorage fromFile(String filepath) throws FileException {
        return new MockPageStorage(filepath);
    }
}