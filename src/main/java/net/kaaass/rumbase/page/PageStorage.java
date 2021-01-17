package net.kaaass.rumbase.page;

/**
 * 用于管理一系列连续页的存储对象，隐藏任何关于存储的物理细节
 *
 * @author XuanLaoYee
 */
public interface PageStorage {

    /**
     * 获取该页存储中的某一页
     *
     * @param pageId 页号
     * @return 该页页对象
     */
    Page get(long pageId);

    /**
     * 将页存储中的所有脏页写回文件
     */
    void flush();
}
