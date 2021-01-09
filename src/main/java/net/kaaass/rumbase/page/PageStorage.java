package net.kaaass.rumbase.page;

import net.kaaass.rumbase.page.exception.FileException;

/**
 * 用于管理一系列连续页的存储对象，隐藏任何关于存储的物理细节
 *
 * @author XuanLaoYee
 */
public interface PageStorage {

    /**
     * 获取该页存储中的某一页
     * @param pageId 页号
     * @return 该页页对象
     * @throws FileException TODO 为什么要抛出FileException？此步骤应该隐藏文件接口的任何细节
     */
    Page get(long pageId) throws FileException;

    /**
     * 将页存储中的所有脏页写回文件
     */
    void flush();
}
