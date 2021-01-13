package net.kaaass.rumbase.table;

import lombok.*;
import net.kaaass.rumbase.exception.RumbaseException;
import net.kaaass.rumbase.record.IRecordStorage;
import net.kaaass.rumbase.record.mock.MockRecordStorage;
import net.kaaass.rumbase.table.field.BaseField;
import net.kaaass.rumbase.table.exception.TableNotFoundException;
import net.kaaass.rumbase.table.exception.TableConflictException;
import net.kaaass.rumbase.transaction.TransactionContext;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.*;

/**
 * 表结构
 * <p>
 * 验证数据是否满足表约束
 * <p>
 * 提供行数据解析服务
 *
 * @author @KveinAxel
 */
@RequiredArgsConstructor
public class Table {

    /**
     * 表名
     */
    @NonNull
    String tableName;

    /**
     * 表所在文件的记录接口
     */
    IRecordStorage recordStorage = null;

    /**
     * 表类型的uuid
     */
    UUID selfUuid = null;

    /**
     * 表的状态，可能的状态有：
     * <p>
     * 正常
     * </p>
     * <p>
     * 被删除
     * </p>
     */
    @Getter
    TableStatus status = TableStatus.NORMAL;

    /**
     * 下一张表的uuid
     */
    @Setter
    @Getter
    UUID next = null;

    /**
     * 表结构拥有的字段
     */
    @Getter
    @Setter
    List<BaseField> baseFields = new ArrayList<>();

    /**
     * 直接通过表名、字段创建表
     * <p>
     * 不检测是否与已存在表冲突，这个留给表管理器处检测
     * </p>
     *
     * @param tableName 表名
     * @param baseFields    表的字段结构
     */
    public Table(@NonNull String tableName, @NonNull List<BaseField> baseFields) {
        // fixme remove mock
        this.recordStorage = MockRecordStorage.ofFile(tableName);
        this.tableName = tableName;
        this.baseFields = baseFields;
        this.status = TableStatus.NORMAL;
    }

    /**
     * 根据配置信息解析获取表属性
     *
     * @param raw 配置信息
     */
    public void parseSelf(byte[] raw) {
        // todo
    }

    /**
     * 将自己持久化成字节数组
     *
     * @return 字节数组
     */
    public byte[] persistSelf() {
        // todo
        return new byte[0];
    }

    /**
     * 创建当前表
     *
     * @param context 事务context
     */
    public void create(TransactionContext context) {
        // todo
    }

    /**
     * 删除元组
     *
     * @param context 事务context
     * @param uuid    元组的uuid
     */
    public void delete(TransactionContext context, UUID uuid) {
        recordStorage.delete(context, uuid);
    }

    /**
     * 更新元组
     *
     * @param context  事务context
     * @param uuid     元组的uuid
     * @param entry    新的行的字符串值列表
     */
    public void update(TransactionContext context, UUID uuid, List<String> entry) throws RumbaseException {

        if(!checkStringEntry(entry)) {
            throw new TableConflictException(3);
        }

        var raw = stringEntryToBytes(entry);

        recordStorage.delete(context, uuid);
        var newUuid = recordStorage.insert(context, raw);

        // todo 更新索引
    }


    /**
     * 检查一个entry是否满足当前表的约束
     *
     * @param entry 待检查entry
     * @return 满足情况
     */
    boolean checkStringEntry(List<String> entry) {
        if (baseFields.size() != entry.size()) {
            return false;
        }

        var len = baseFields.size();

        for (int i = 0; i < len; i++) {
            if (!baseFields.get(i).checkStr(entry.get(i))) {
                return false;
            }
        }

        return true;
    }

    /**
     * 获取一个元组内容
     *
     * @param context 事务context
     * @param uuid    元组的uuid
     * @return 元组
     * @throws TableNotFoundException         要查询的表不存在
     * @throws TableConflictException 查询到的entry和当前表冲突
     */
    public List<Object> read(TransactionContext context, UUID uuid) throws TableNotFoundException, TableConflictException {

        var bytes = recordStorage
                .queryOptional(context, uuid)
                .orElseThrow(() -> new TableNotFoundException(4));

        try {
            return parseEntry(bytes);
        } catch (IOException e) {
            // fixme 不该出现这样的事情（吧）
            // 查询到的entry和当前表冲突
            throw new TableConflictException(3);
        }

    }

    /**
     * 向表插入元组
     *
     * @param context  事务context
     * @param entry 新的元组
     * @throws TableConflictException 插入的元组不满足表约束
     */
    public void insert(TransactionContext context, List<String> entry) throws TableConflictException {

        var bytes = stringEntryToBytes(entry);

        var uuid = recordStorage.insert(context, bytes);

        // todo add to index
    }

    /**
     * @param fieldName 字段名
     * @param left      查询区间左端点
     * @param right     查询区间右端点
     * @return 查询到的uuid列表
     * @throws TableNotFoundException 要查询的表不存在
     */
    public List<UUID> search(String fieldName, BaseField left, BaseField right) throws TableNotFoundException {

        throw new TableNotFoundException(4);
    }

    /**
     * 将entry转换成字节数组
     *
     * @param entry 元组
     * @return 字节数组
     */
    byte[] stringEntryToBytes(List<String > entry) throws TableConflictException {
        var stream = new ByteArrayOutputStream();

        if (!checkStringEntry(entry)) {
            throw new TableConflictException(3);
        }

        var len = baseFields.size();

        for (int i = 0; i < len; i++) {
            baseFields.get(i).serialize(stream, entry.get(i));
        }

        return stream.toByteArray();
    }

    /**
     * 将字节数组转换成entry
     *
     * @param raw 字节数组
     * @return 元组
     */
    List<Object> parseEntry(byte[] raw) throws TableConflictException, IOException {
        var stream = new ByteArrayInputStream(raw);
        var list = new ArrayList<>();

        for (var field : baseFields) {
            list.add(field.deserialize(stream));
        }

        if (stream.available() == 0) {
            return list;
        } else {
            throw new TableConflictException(2);
        }
    }

}