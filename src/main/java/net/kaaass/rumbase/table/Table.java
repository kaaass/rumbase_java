package net.kaaass.rumbase.table;

import com.igormaznitsa.jbbp.JBBPParser;
import lombok.*;
import net.kaaass.rumbase.record.IRecordStorage;
import net.kaaass.rumbase.record.mock.MockRecordStorage;
import net.kaaass.rumbase.table.exception.NotFoundException;
import net.kaaass.rumbase.table.exception.TypeIncompatibleException;
import net.kaaass.rumbase.transaction.TransactionContext;

import java.io.IOException;
import java.util.*;

import static com.igormaznitsa.jbbp.io.JBBPOut.BeginBin;

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
    UUID selfUUID = null;

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
    List<Field> fields = new ArrayList<>();

    /**
     * 直接通过表名、字段创建表
     * <p>
     * 不检测是否与已存在表冲突，这个留给表管理器处检测
     * </p>
     *
     * @param tableName 表名
     * @param fields    表的字段结构
     */
    public Table(@NonNull String tableName, @NonNull List<Field> fields) {
        this.recordStorage = MockRecordStorage.ofFile(tableName);
        this.tableName = tableName;
        this.fields = fields;
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
     * @param newEntry 新的元组
     */
    public void update(TransactionContext context, UUID uuid, Entry newEntry) throws TypeIncompatibleException {


        if(!checkEntry(newEntry))
            throw new TypeIncompatibleException(3);

        var raw = entryToRaw(newEntry);

        recordStorage.delete(context, uuid);
        var newUUID = recordStorage.insert(context, raw);

        // todo 更新索引
    }


    /**
     * 检查一个entry是否满足当前表的约束
     *
     * @param entry 待检查entry
     * @return 满足情况
     */
    boolean checkEntry(Entry entry) {
        if (fields.size() != entry.size()) {
            return false;
        }

        return fields
                .stream()
                .allMatch(f -> f.checkValue(entry.get(f.getName())));
    }

    /**
     * 获取一个元组内容
     *
     * @param context 事务context
     * @param uuid    元组的uuid
     * @return 元组
     * @throws NotFoundException         要查询的表不存在
     * @throws TypeIncompatibleException 查询到的entry和当前表冲突
     */
    public Entry read(TransactionContext context, UUID uuid) throws NotFoundException, TypeIncompatibleException {

        var bytes = recordStorage
                .queryOptional(context, uuid)
                .orElseThrow(() -> new NotFoundException(4));

        try {
            return parseEntry(bytes);
        } catch (IOException e) {
            // fixme 不该出现这样的事情（吧）
            // 查询到的entry和当前表冲突
            throw new TypeIncompatibleException(3);
        }

    }

    /**
     * 向表插入元组
     *
     * @param context  事务context
     * @param newEntry 新的元组
     * @throws TypeIncompatibleException 插入的元组不满足表约束
     */
    public void insert(TransactionContext context, Entry newEntry) throws TypeIncompatibleException {

        var raw = entryToRaw(newEntry);

        var uuid = recordStorage.insert(context, raw);
        // todo add to index
    }

    /**
     * @param fieldName 字段名
     * @param left      查询区间左端点
     * @param right     查询区间右端点
     * @return 查询到的uuid列表
     * @throws NotFoundException 要查询的表不存在
     */
    public List<UUID> search(String fieldName, FieldValue left, FieldValue right) throws NotFoundException {

        return fields
                .stream()
                .filter(f -> checkSearchValue(fieldName, f, left, right))
                .findAny()
                .orElseThrow(() -> new NotFoundException(2))
                .searchRange(left, right);
    }

    /**
     * 存在名字相同的域, 且左右值的类型与该域相同,如果左右值都存在，则按<=的偏序关系传入
     *
     * @param fieldName 目标字段的名字
     * @param field     待检查字段
     * @param left      区间左端点
     * @param right     区间右端点
     * @return 满足情况
     */
    boolean checkSearchValue(String fieldName, Field field, FieldValue left, FieldValue right) {
        return fieldName.equals(field.getName()) // 名字符合
                && (left == null || field.checkValue(left)) // 如果存在则需满足约束
                && (right == null || field.checkValue(right)) // 如果存在则需满足约束
                && (left == null || right == null || left.compareTo(right) <= 0); // 如果都存在，需要满足偏序关系
    }

    /**
     * 将字符串列表转换成一个元组
     *
     * @param values 字符串列表
     * @return 元组的
     */
    public Entry strToEntry(List<String> values) throws TypeIncompatibleException {
        var entry = new Entry();

        if (values.size() != fields.size()) {
            throw new TypeIncompatibleException(1);
        }

        var iter = values.iterator();

        for (var field : fields) {
            var valStr = iter.next();
            if (field.checkStr(valStr))
                entry.put(field.getName(), field.strToValue(valStr));
            else
                throw new TypeIncompatibleException(1);
        }

        return entry;
    }

    /**
     * 将entry转换成字节数组
     *
     * @param entry 元组
     * @return 字节数组
     */
    public byte[] entryToRaw(Entry entry) throws TypeIncompatibleException {
        var jbbpOut = BeginBin();

        if (!checkEntry(entry))
            throw new TypeIncompatibleException(3);

        try {
            for (var e : entry.entrySet()) {
                e.getValue().append2JBBPOut(jbbpOut);
            }
            return jbbpOut.End().toByteArray();

        } catch (IOException e) {
            // fixme 返回的异常可能不太合适
            throw new TypeIncompatibleException(3);
        }
    }

    /**
     * 将字节数组转换成entry
     *
     * @param raw 字节数组
     * @return 元组
     */
    public Entry parseEntry(byte[] raw) throws TypeIncompatibleException, IOException {
        var entry = new Entry();
        var stringBuilder = new StringBuilder();

        for (var field : fields) {
            stringBuilder.append(field.getPrepareCode());
        }

        var struct = JBBPParser.prepare(stringBuilder.toString()).parse(raw);

        for (var field : fields) {
            var value = field.JBBPStructToValue(struct);
            entry.put(field.getName(), value);
        }

        return entry;
    }

}