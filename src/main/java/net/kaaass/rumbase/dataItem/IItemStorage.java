package net.kaaass.rumbase.dataItem;

import net.kaaass.rumbase.dataItem.exception.UUIDException;

import java.util.List;

/**
 * 数据项管理接口
 *
 * @author  kaito
 */
public interface IItemStorage {
    /**
     * 插入数据项
     * @param item 数据项
     * @return 返回数据项的UUID
     */
    long insertItem(byte[] item);

    /**
     *  插入一个有UUID的数据项，唯一使用的地方是日志恢复时使用
     * @param item 数据项
     * @param uuid
     */
    void insertItemWithUUID(byte[] item,long uuid) throws UUIDException;

    /**
     * 通过UUID查询数据项
     * @param uuid
     * @return  数据项
     */
    byte[] queryItemByUUID(long uuid) throws UUIDException;

    /**
     *  通过页号获得数据项纪录
     * @param pageID 页号
     * @return list的一堆数据项
     */
    List<byte[]> queryItemByPageID(int pageID);

    /**
     *  通过UUID更新数据项
     * @param uuid
     * @param item
     */
    void updateItemByUUID(long uuid,byte[] item) throws UUIDException;

    byte[] getMetadata();

    void setMetadata(byte[] metadata);

    /**
     *  清理多余的数据项，空间清理时使用。
     * @param uuids 数据项UUID的编号列表
     *
     */
    void removeItems(List<Long> uuids);
}


