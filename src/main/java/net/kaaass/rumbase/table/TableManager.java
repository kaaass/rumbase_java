package net.kaaass.rumbase.table;

import lombok.NoArgsConstructor;
import net.kaaass.rumbase.table.exception.NotFoundException;
import net.kaaass.rumbase.table.exception.TypeIncompatibleException;
import net.kaaass.rumbase.transaction.TransactionContext;

import java.io.IOException;
import java.util.*;

/**
 * 表管理器
 * <p>
 * ITableManager用于管理表结构, 已经为上层模块提供更加高级和抽象的接口.
 * </p><p>
 * ITableManager会依赖index模块进行索引, 依赖record模块进行表单数据查找.
 * </p>
 *
 * @author @KveinAxel
 */
@NoArgsConstructor
public class TableManager {

    /**
     * 对所有的表结构的缓存
     * <p>
     * 如果不在这里，说明就没有这张表
     * <p>
     * 同样地，创建表、删除表需要维护这个结构
     */
    private final Map<String, Table> tableCache = new HashMap<>();

    /**
     * 事务开启的表
     */
    private final Map<TransactionContext, List<Table>> transactionTableCache = new HashMap<>();

    /**
     * 通过系统配置表读取所有的表
     */
    public void loadMeta() {

        // todo 读取并加载配置表
    }

    /**
     * 开始一个事务
     *
     * @param IsRepeatableRead 是否可重复读
     * @return 事务context
     */
    TransactionContext begin(boolean IsRepeatableRead) {
        return TransactionContext.empty();
    }

    /**
     * 提交一个事务
     *
     * @param context 事务context
     */
    void commit(TransactionContext context) {
        // todo commit tx
    }

    /**
     * 终止一个事务
     *
     * @param context 事务context
     */
    void abort(TransactionContext context) {
        // todo abort tx
    }

    /**
     * 显示所有的表名
     *
     * @return 表名的列表
     */
    List<String> showTables() {
        List<String> list = new ArrayList<>();

        tableCache.forEach((k, v) -> list.add(k));

        return list;
    }

    /**
     * 创建一个表
     *
     * @param context   事务context
     * @param tableName 表名
     * @param fields    表的字段
     * @throws TypeIncompatibleException 该表已存在
     */
    void createTable(
            TransactionContext context,
            String tableName,
            List<Field> fields
    ) throws TypeIncompatibleException {
        if (tableCache.containsKey(tableName)) {
            throw new TypeIncompatibleException(4);
        }

        var table = new Table(tableName, fields);
        table.create(context);

        tableCache.put(tableName, table);
    }

    /**
     * 向一张表插入数据
     *
     * @param context   事务context
     * @param tableName 表名
     * @param newEntry  新的Entry
     * @throws NotFoundException         不存在该表
     * @throws TypeIncompatibleException 插入的Entry与表字段冲突
     */
    void insert(
            TransactionContext context,
            String tableName,
            Entry newEntry
    ) throws NotFoundException, TypeIncompatibleException {

        var table = tableCache.get(tableName);

        if (table == null) {
            throw new NotFoundException(1);
        }

        table.insert(context, newEntry);
    }

    /**
     * 删除一张表的数据
     *
     * @param context   事务context
     * @param tableName 表名
     * @param uuids     待删除的元组的uuid列表
     * @throws NotFoundException 不存在该表
     */
    void delete(
            TransactionContext context,
            String tableName,
            List<UUID> uuids
    ) throws NotFoundException {

        var table = tableCache.get(tableName);

        if (table == null) {
            throw new NotFoundException(1);
        }

        uuids.forEach(uuid -> table.delete(context, uuid));
    }

    /**
     * 更新一张表的数据
     *
     * @param context    事务context
     * @param tableName  表名
     * @param newEntries 待更新的entry，键为旧行的uuid，值为新行的entry
     * @throws NotFoundException 不存在该表
     * @throws TypeIncompatibleException 插入的Entry与表字段冲突
     */
    void update(
            TransactionContext context,
            String tableName,
            Map<UUID, Entry> newEntries
    ) throws NotFoundException, TypeIncompatibleException {
        var table = tableCache.get(tableName);

        if (table == null) {
            throw new NotFoundException(1);
        }

        for (var e : newEntries.entrySet()) {
            table.update(context, e.getKey(), e.getValue());
        }
    }

    /**
     * 读取一张表的数据
     *
     * @param context   事务context
     * @param tableName 表名
     * @param uuids     待查询的uuid的列表
     * @return 查询到的行的Entry
     * @throws NotFoundException 不存在该表
     * @throws TypeIncompatibleException 插入的Entry与表字段冲突
     */
    List<Entry> read(
            TransactionContext context,
            String tableName,
            List<UUID> uuids
    ) throws NotFoundException, TypeIncompatibleException {

        var table = tableCache.get(tableName);
        var entryList = new ArrayList<Entry>();

        if (table == null) {
            throw new NotFoundException(1);
        }

        for (var uuid : uuids) {
            entryList.add(table.read(context, uuid));
        }

        return entryList;
    }

    /**
     * 查询满足 [left, right) 区间uuid列表:
     * <p>
     * 如果 left 为空，则查询 [ begin, right )
     * <p>
     * 如果 right 为空，则查询 [ left, end )
     * <p>
     * 如果 left, right 都为空，查询 [ begin, end )
     * <p>
     *
     * @param context   事务context
     * @param tableName 表名
     * @param fieldName 字段名
     * @param left      查询区间左端点
     * @param right     查询区间右端点
     * @return 查询到的uuid
     * @throws NotFoundException 不存在该表
     */
    List<UUID> search(
            TransactionContext context,
            String tableName,
            String fieldName,
            FieldValue left,
            FieldValue right
    ) throws NotFoundException {

        var table = tableCache.get(tableName);

        if (table == null) {
            throw new NotFoundException(1);
        }

        return table.search(fieldName, left, right);
    }
}
