package net.kaaass.rumbase.dataitem;

import net.kaaass.rumbase.dataitem.exception.PageCorruptedException;
import net.kaaass.rumbase.dataitem.exception.UUIDException;
import net.kaaass.rumbase.page.exception.FileException;
import net.kaaass.rumbase.page.exception.PageException;
import net.kaaass.rumbase.recovery.IRecoveryStorage;
import net.kaaass.rumbase.recovery.exception.LogException;
import net.kaaass.rumbase.transaction.TransactionContext;

import java.io.IOException;
import java.util.List;

/**
 * 数据项管理接口
 *
 * @author kaito
 */
public interface IItemStorage {

    void setMetaUuid(long uuid) throws PageException, IOException;

    /**
     * 获得日志管理器
     *
     * @return
     */
    IRecoveryStorage getRecoveryStorage();

    /**
     * 获取表的tempFreePage
     *
     * @return
     */
    public int getMaxPageId();

    /**
     * 将uuid对应的页强制写回
     */
    public void flush(long uuid) throws FileException;

    /**
     * 插入数据项
     *
     * @param txContext 事务上下文
     * @param item      数据项
     * @return 返回数据项的UUID
     */
    long insertItem(TransactionContext txContext, byte[] item) throws PageCorruptedException;

    /**
     * 不用日志进行插入，用于日志的管理
     *
     * @param item 数据项
     * @return uuid
     */
    long insertItemWithoutLog(byte[] item);

    /**
     * 插入一个有UUID的数据项，唯一使用的地方是日志恢复时使用
     * <p>
     * 如果数据项已经存在，就将数据更新在已存在的数据项所在空间上；
     * 如果数据项不存在，则以此UUID插入数据项
     * </p>
     *
     * @param item 数据项
     * @param uuid 编号
     */
    void insertItemWithUuid(byte[] item, long uuid);

    /**
     * 通过UUID查询数据项
     *
     * @param uuid 编号
     * @return 数据项
     * @throws UUIDException UUID找不到的异常
     */
    byte[] queryItemByUuid(long uuid) throws UUIDException;


    /**
     * 列出页中所有的记录
     *
     * @param pageId 页号
     * @return list的一堆数据项
     */
    List<byte[]> listItemByPageId(int pageId);

    /**
     * 根据UUID更新数据项
     *
     * @param uuid 编号
     * @param item 数据项
     * @throws UUIDException 没有找到对应UUID的异常
     */
    void updateItemByUuid(TransactionContext txContext, long uuid, byte[] item) throws UUIDException, PageCorruptedException;

    byte[] updateItemWithoutLog(long uuid, byte[] item) throws UUIDException;

    /**
     * 获得数据项存储的元数据（可以用于头）
     *
     * @return 元数据
     */
    byte[] getMetadata();

    /**
     * 设置数据项存储的元数据（可以用于头）
     *
     * @param metadata 头信息
     */
    long setMetadata(TransactionContext txContext, byte[] metadata);

    /**
     * 不使用日志设置元数据
     *
     * @param metadata 头信息
     */
    long setMetadataWithoutLog(byte[] metadata);

    /**
     * 清理多余的数据项，空间清理时使用。
     *
     * @param uuids 数据项UUID的编号列表
     */
    void removeItems(List<Long> uuids);

    /**
     * 日志恢复时用，回退对应的insert操作，做法是删除对应的uuid
     *
     * @param uuid
     */
    void deleteUuid(long uuid) throws PageException, IOException;

    /**
     * 建议存储进行一次回写
     */
    void flush();
}


