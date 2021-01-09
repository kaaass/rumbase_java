package net.kaaass.rumbase.record;

import net.kaaass.rumbase.record.exception.RecordNotFoundException;
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
    long insert(TransactionContext txContext, byte[] rawData);

    /**
     * 由记录ID查询记录数据
     *
     * @param txContext 事务上下文
     * @param recordId  记录ID
     * @return 记录数据字节
     * @throws RecordNotFoundException 若记录不存在、不可见，抛出错误
     */
    byte[] query(TransactionContext txContext, long recordId) throws RecordNotFoundException;

    /**
     * 由记录ID查询记录数据，并忽略由事务造成的记录不可见。若物理记录不存在，将抛出运行时错误
     *
     * @param txContext 事务上下文
     * @param recordId  记录ID
     * @return 记录数据字节，若记录不可见则返回Optional.empty()
     */
    Optional<byte[]> queryOptional(TransactionContext txContext, long recordId);

    /**
     * 由记录ID删除记录数据
     *
     * @param txContext 事务上下文
     * @param recordId  记录ID
     */
    void delete(TransactionContext txContext, long recordId);

    /**
     * 获得记录存储的元信息（与单个记录无关）
     *
     * @return 元信息数据
     */
    byte[] getMetadata(TransactionContext txContext);

    /**
     * 更新记录存储的元信息
     *
     * @param metadata 元信息数据
     */
    void setMetadata(TransactionContext txContext, byte[] metadata);
}
