package net.kaaass.rumbase.record;

import net.kaaass.rumbase.transaction.TransactionContext;

import java.util.Optional;
import java.util.UUID;

/**
 * 记录存储接口，提供由记录ID存取数据的容器
 *
 * @author kaaass
 */
public interface IRecordStorage {

    /**
     * 向存储中加入一串记录数据字节
     *
     * @param txContext 事务上下文
     * @param rawData   字节数据
     * @return 记录ID
     */
    UUID insert(TransactionContext txContext, byte[] rawData);

    /**
     * 由记录ID查询记录数据
     *
     * @param txContext 事务上下文
     * @param recordId  记录ID
     * @return 记录数据字节
     */
    Optional<byte[]> query(TransactionContext txContext, UUID recordId);

    /**
     * 由记录ID删除记录数据
     *
     * @param txContext 事务上下文
     * @param recordId  记录ID
     */
    void delete(TransactionContext txContext, UUID recordId);


    /**
     * 获得记录存储的元信息（与单个记录无关）
     *
     * @return 元信息数据
     */
    byte[] getMetadata();

    /**
     * 更新记录存储的元信息
     *
     * @param metadata 元信息数据
     */
    void setMetadata(byte[] metadata);
}
