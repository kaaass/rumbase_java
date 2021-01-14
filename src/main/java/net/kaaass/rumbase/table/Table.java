package net.kaaass.rumbase.table;

import com.igormaznitsa.jbbp.io.JBBPBitInputStream;
import com.igormaznitsa.jbbp.io.JBBPBitOutputStream;
import com.igormaznitsa.jbbp.io.JBBPByteOrder;
import lombok.*;
import net.kaaass.rumbase.exception.RumbaseException;
import net.kaaass.rumbase.index.Pair;
import net.kaaass.rumbase.record.IRecordStorage;
import net.kaaass.rumbase.record.RecordManager;
import net.kaaass.rumbase.record.mock.MockRecordStorage;
import net.kaaass.rumbase.table.field.BaseField;
import net.kaaass.rumbase.table.exception.TableExistenceException;
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
    @Getter
    String tableName;

    /**
     * 表所在文件的记录接口
     */
    @Getter
    @NonNull
    IRecordStorage recordStorage;

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
    long next;

    /**
     * 表结构拥有的字段
     */
    @Getter
    @Setter
    List<BaseField> fields = new ArrayList<>();

    /**
     * 直接通过表名、字段创建表
     * <p>
     * 不检测是否与已存在表冲突，这个留给表管理器处检测
     * </p>
     *
     * @param tableName 表名
     * @param fields    表的字段结构
     */
    public Table(@NonNull String tableName, @NonNull List<BaseField> fields) {
        // fixme remove mock
        this.recordStorage = MockRecordStorage.ofFile(tableName);
        this.tableName = tableName;
        this.fields = fields;
        this.status = TableStatus.NORMAL;
        this.next = -1;
    }

    /**
     * 将当前表结构信息持久化到外存中
     */
    public void persist(TransactionContext context) {

        var byteOutStream = new ByteArrayOutputStream();
        var out = new JBBPBitOutputStream(byteOutStream);

        try {
            out.writeString(tableName, JBBPByteOrder.BIG_ENDIAN);
            out.writeString(status.toString().toUpperCase(Locale.ROOT), JBBPByteOrder.BIG_ENDIAN);
            out.writeLong(next, JBBPByteOrder.BIG_ENDIAN);
            out.writeInt(fields.size(), JBBPByteOrder.BIG_ENDIAN);
            for (var f: fields) {
                f.persist(byteOutStream);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        recordStorage.setMetadata(context, byteOutStream.toByteArray());
    }

    public static Table load(String tableName) {

        var recordStorage = RecordManager.fromFile(tableName);

        // fixme context
        var context = TransactionContext.empty();
        var meta = recordStorage.getMetadata(context);
        var stream = new ByteArrayInputStream(meta);
        var in = new JBBPBitInputStream(stream);
        try {
            var name = in.readString(JBBPByteOrder.BIG_ENDIAN);
            var status = in.readString(JBBPByteOrder.BIG_ENDIAN);
            var next = in.readLong(JBBPByteOrder.BIG_ENDIAN);
            var fieldNum = in.readInt(JBBPByteOrder.BIG_ENDIAN);
            var fieldList = new ArrayList<BaseField>();
            var table = new Table(name, fieldList);
            for (int i = 0; i < fieldNum; i++) {
                var f = BaseField.load(stream, table);
                if (f != null) {
                    fieldList.add(f);
                }
            }
            table.next = next;
            table.status = TableStatus.valueOf(status);
            return table;
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 删除元组
     *
     * @param context 事务context
     * @param uuid    元组的uuid
     */
    public void delete(TransactionContext context, long uuid) {
        recordStorage.delete(context, uuid);
    }

    /**
     * 更新元组
     *
     * @param context  事务context
     * @param uuid     元组的uuid
     * @param entry    新的行的字符串值列表
     */
    public void update(TransactionContext context, long uuid, List<String> entry) throws RumbaseException {

        if(!checkStringEntry(entry)) {
            throw new TableConflictException(3);
        }

        var raw = stringEntryToBytes(entry);

        recordStorage.delete(context, uuid);
        var newUuid = recordStorage.insert(context, raw);

        var l = entry.size();
        for (int i = 0; i < l; i++) {
            var field = fields.get(i);
            if (field.indexed()) {
                field.insertIndex(entry.get(i), newUuid);
            }
        }
    }


    /**
     * 检查一个entry是否满足当前表的约束
     *
     * @param entry 待检查entry
     * @return 满足情况
     */
    public boolean checkStringEntry(List<String> entry) {
        if (fields.size() != entry.size()) {
            return false;
        }

        var len = fields.size();

        for (int i = 0; i < len; i++) {
            if (!fields.get(i).checkStr(entry.get(i))) {
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
     * @throws TableExistenceException         要查询的表不存在
     * @throws TableConflictException 查询到的entry和当前表冲突
     */
    public List<Object> read(TransactionContext context, long uuid) throws TableExistenceException, TableConflictException {

        var bytes = recordStorage
                .queryOptional(context, uuid)
                .orElseThrow(() -> new TableExistenceException(4));

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
    public void insert(TransactionContext context, List<String> entry) throws TableConflictException, TableExistenceException {

        var bytes = stringEntryToBytes(entry);

        var uuid = recordStorage.insert(context, bytes);

        var l = entry.size();
        for (int i = 0; i < l; i++) {
            var field = fields.get(i);
            if (field.indexed()) {
                field.insertIndex(entry.get(i), uuid);
            }
        }
    }

    /**
     * @param fieldName 字段名
     * @param fieldValue 字段值
     * @return 查询到的uuid列表
     * @throws TableExistenceException 要查询的表不存在
     */
    public List<Long> search(String fieldName, String fieldValue) throws TableExistenceException, TableConflictException {
        BaseField field = null;

        for (var f: fields) {
            if (f.getName().equals(fieldName)) {
                field = f;
            }
        }

        if (field == null) {
            throw new TableExistenceException(2);
        }

        return field.queryIndex(fieldValue);
    }

    /**
     * 以指定的索引查询全部记录
     * <p>
     * 返回第一个记录的迭代器，以(字段值, 记录uuid)的方式返回
     * <p>
     * 返回的uuid不保证可见
     *
     * @param fieldName 字段名
     * @return (字段值, 记录uuid)形式的第一个迭代器
     * @throws TableExistenceException 字段不存在(2), 索引不存在(6)
     */
    public Iterator<Pair> searchAll(String fieldName) throws TableExistenceException {
        BaseField field = null;

        for (var f: fields) {
            if (f.getName().equals(fieldName)) {
                field = f;
            }
        }

        if (field == null) {
            throw new TableExistenceException(2);
        }

        return field.queryFirst();
    }


    /**
     * 查询第一个满足字段值的迭代器
     * <p>
     * 返回第一个记录的迭代器，以(字段值, 记录uuid)的方式返回
     * <p>
     * 返回的uuid不保证可见
     *
     * @param fieldName 字段名
     * @param key 字段值
     * @return (字段值, 记录uuid)形式的第一个迭代器
     * @throws TableExistenceException 字段不存在(2), 索引不存在(6)
     * @throws TableConflictException 字段类型不匹配
     */
    public Iterator<Pair> searchFirst(String fieldName, String key) throws TableExistenceException, TableConflictException {
        BaseField field = null;

        for (var f: fields) {
            if (f.getName().equals(fieldName)) {
                field = f;
            }
        }

        if (field == null) {
            throw new TableExistenceException(2);
        }

        return field.queryFirstMeet(key);
    }

    /**
     * 查询第一个满足值且与查询键不相等的迭代器
     * <p>
     * 返回第一个记录的迭代器，以(字段值, 记录uuid)的方式返回
     * <p>
     * 返回的uuid不保证可见
     *
     * @param fieldName 字段名
     * @return (字段值, 记录uuid)形式的第一个迭代器
     * @throws TableExistenceException 字段不存在(2), 索引不存在(6)
     */
    public Iterator<Pair> searchFirstNotEqual(String fieldName, String key) throws TableExistenceException, TableConflictException {
        BaseField field = null;

        for (var f: fields) {
            if (f.getName().equals(fieldName)) {
                field = f;
            }
        }

        if (field == null) {
            throw new TableExistenceException(2);
        }

        return field.queryFirstMeetNotEqual(key);
    }

    /**
     * 将entry转换成字节数组
     *
     * @param entry 元组
     * @return 字节数组
     */
    public byte[] stringEntryToBytes(List<String> entry) throws TableConflictException {
        var stream = new ByteArrayOutputStream();

        if (!checkStringEntry(entry)) {
            throw new TableConflictException(3);
        }

        var len = fields.size();

        for (int i = 0; i < len; i++) {
            fields.get(i).serialize(stream, entry.get(i));
        }

        return stream.toByteArray();
    }

    /**
     * 将字节数组转换成entry
     *
     * @param raw 字节数组
     * @return 元组
     */
    public List<Object> parseEntry(byte[] raw) throws TableConflictException, IOException {
        var stream = new ByteArrayInputStream(raw);
        var list = new ArrayList<>();

        for (var field : fields) {
            list.add(field.deserialize(stream));
        }

        if (stream.available() == 0) {
            return list;
        } else {
            throw new TableConflictException(2);
        }
    }

}