package net.kaaass.rumbase.page;

import net.kaaass.rumbase.page.exception.FileExeception;

import java.util.HashMap;
import java.util.Map;

/**
 * 以文件名初始化
 */
public interface PageCache {

    public Page get(long pageId) throws FileExeception;

    /**
     * 以文件的方式写回文件
     */
    public void flush();
}
