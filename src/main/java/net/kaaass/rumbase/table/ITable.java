package net.kaaass.rumbase.table;

import net.kaaass.rumbase.record.IRecordStorage;
import net.kaaass.rumbase.table.exception.TableNotFoundException;
import net.kaaass.rumbase.table.exception.TypeIncompatibleException;
import net.kaaass.rumbase.transaction.TransactionContext;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * 表结构
 * <p>
 * 验证数据是否满足表约束
 * <p>
 * 提供行数据解析服务
 *
 * @author @KveinAxel
 */
public interface ITable {

    /**
     * 根据配置信息解析获取表属性
     * @param raw 配置信息
     */
    void parseSelf(byte[] raw);

    /**
     * 将自己持久化成字节数组
     */
    byte[] persistSelf();

    /**
     * 删除元组
     *
     * @param context 事务context
     * @param fieldName 字段名
     * @param uuid 元组的uuid
     * @param recordStorage record层访问接口
     * @return 被删除的元组数
     */
    int delete(TransactionContext context, String fieldName, UUID uuid, IRecordStorage recordStorage);

    /**
     * 更新元组
     *
     * @param context 事务context
     * @param fieldName 字段名
     * @param uuid 元组的uuid
     * @param newEntry 新的元组
     * @return 被更新的元组数
     */
    int update(TransactionContext context, String fieldName, UUID uuid, Entry newEntry);

    /**
     * 获取元组内容
     *
     * @param context 事务context
     * @param fieldName 字段名
     * @param uuids 元组的uuid列表
     * @param recordStorage record层访问接口
     * @return 元组entry列表
     * @throws TableNotFoundException 要查询的表不存在
     */
    List<Entry> read(TransactionContext context, String fieldName, List<UUID> uuids, IRecordStorage recordStorage) throws TableNotFoundException;

    /**
     * 向表插入元组
     *
     * @param context 事务context
     * @param fieldName 字段名
     * @param newEntry 新的元组
     * @param recordStorage record层访问接口
     * @throws TableNotFoundException 要查询的表不存在
     * @throws TypeIncompatibleException 插入的元组不满足表约束
     */
    void insert(TransactionContext context, String fieldName, Entry newEntry, IRecordStorage recordStorage) throws TableNotFoundException, TypeIncompatibleException;

    /**
     *
     *
     * @param fieldName 字段名
     * @param left 查询区间左端点
     * @param right 查询区间右端点
     * @return 查询到的uuid列表
     * @throws TableNotFoundException 要查询的表不存在
     */
    List<UUID> search(String fieldName, FieldValue left, FieldValue right) throws TableNotFoundException;

    /**
     * 将字符串列表转换成一个元组
     *
     * @param values 字符串列表
     * @return 元组的Optional
     */
    Optional<Entry> strToEntry(List<String> values);

    /**
     * 将entry转换成字节数组
     *
     * @param entry 元组
     * @return 字节数组
     */
    Optional<byte[]> entryToRaw(Entry entry);

    /**
     * 将字节数组转换成entry
     *
     * @param raw 字节数组
     * @return 元组
     */
    Optional<Entry> parseEntry(byte[] raw);

}
