package net.kaaass.rumbase.dataitem;

import net.kaaass.rumbase.dataitem.exception.UUIDException;

import java.util.List;
import java.util.Optional;

/**
 * 数据项管理接口
 *
 * @author kaito
 */
public interface IItemStorage {
    /**
     * 插入数据项
     *
     * @param item 数据项
     * @return 返回数据项的UUID
     */
    long insertItem(byte[] item);

    /**
     * 插入一个有UUID的数据项，唯一使用的地方是日志恢复时使用
     * <p>
     * 如果数据项已经存在，就将数据更新在已存在的数据项所在空间上；
     * 如果数据项不存在，则以此UUID插入数据项
     *
     * @param item 数据项
     * @param uuid
     */
    void insertItemWithUUID(byte[] item, long uuid) throws UUIDException;

    /**
     * 通过UUID查询数据项 FIXME 不要因为记录不存在丢出错误，性能问题。直接返回null
     *
     * @param uuid
     * @return 数据项
     */
    byte[] queryItemByUUID(long uuid);

    /**
     * 通过UUID查询数据项
     *
     * @param uuid
     * @return Optional形式数据项
     */
    default Optional<byte[]> queryItemOptional(long uuid) {
        return Optional.ofNullable(queryItemByUUID(uuid));
    }

    /**
     * 列出页中所有的记录
     *
     * @param pageID 页号
     * @return list的一堆数据项
     */
    List<byte[]> listItemByPageId(int pageID);

    /**
     * 通过UUID更新数据项 FIXME 不要因为记录不存在丢出错误，性能问题。直接返回null。修改方法参考上面的，加一个updateItemOptional
     *
     * @param uuid
     * @param item
     */
    void updateItemByUUID(long uuid, byte[] item) throws UUIDException;

    /**
     * 获得数据项存储的元数据（可以用于头）
     *
     * @return 元数据
     */
    byte[] getMetadata();

    /**
     * 设置数据项存储的元数据（可以用于头）
     *
     * @param metadata
     */
    void setMetadata(byte[] metadata);

    /**
     * 清理多余的数据项，空间清理时使用。
     *
     * @param uuids 数据项UUID的编号列表
     */
    void removeItems(List<Long> uuids);
}


