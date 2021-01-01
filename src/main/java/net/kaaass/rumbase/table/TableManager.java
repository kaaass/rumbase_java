package net.kaaass.rumbase.table;

import net.kaaass.rumbase.record.IRecordStorage;
import net.kaaass.rumbase.table.exception.TableNotFoundException;
import net.kaaass.rumbase.table.exception.TypeIncompatibleException;
import net.kaaass.rumbase.transaction.TransactionContext;

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
public class TableManager {
    
//    private DataItem dataItem;

    private final IRecordStorage recordStorage;

    private Map<String, ITable> tableCache;

    private Map<TransactionContext, List<ITable>> transactionTableCache;

//    private Lock lock;

    public TableManager(IRecordStorage recordStorage) {
        this.recordStorage = recordStorage;
        this.tableCache = new HashMap<>();
        this.tableCache = new HashMap<>();
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
        // todo show tables
        return new ArrayList<>();
    }

    /**
     * 创建一个表
     *
     * @param context 事务context
     * @param tableName 表名
     * @param fields 表的字段
     */
    void createTable(TransactionContext context, String tableName, List<IField> fields) {
        // todo create Table
    }

    /**
     * 向一张表插入数据
     *
     * @param context 事务context
     * @param tableName 表名
     * @param fieldName 字段名
     * @param newEntry 新的Entry
     * @throws TableNotFoundException 不存在该表
     * @throws TypeIncompatibleException 插入的Entry与表字段冲突
     */
    void insert(TransactionContext context, String tableName, String fieldName, Entry newEntry) throws TableNotFoundException, TypeIncompatibleException {
        var table = tableCache.get(tableName);
        if (table == null) {
            throw new TableNotFoundException(1);
        }

        table.insert(context, fieldName, newEntry, recordStorage);
    }

    /**
     * 删除一张表的数据
     *
     * @param context 事务context
     * @param tableName 表名
     * @param fieldName 字段名
     * @param uuids 待删除的元组的uuid列表
     * @return 被删除的元组数
     * @throws TableNotFoundException 不存在该表
     */
    int delete(TransactionContext context, String tableName, String fieldName, List<UUID> uuids) throws TableNotFoundException {
        var table = tableCache.get(tableName);
        if (table == null) {
            throw new TableNotFoundException(1);
        }

        return uuids.stream()
                .map(uuid -> table.delete(context, fieldName, uuid, recordStorage))
                .reduce(0, Integer::sum);
    }

    /**
     * 更新一张表的数据
     *
     * @param context 事务context
     * @param tableName 表名
     * @param fieldName 字段名
     * @param newEntries 待更新的entry，键为旧行的uuid，值为新行的entry
     * @return 被更新的行数
     * @throws TableNotFoundException 不存在该表
     */
    int update(TransactionContext context, String tableName, String fieldName, Map<UUID, Entry> newEntries) throws TableNotFoundException {
        var table = tableCache.get(tableName);
        if (table == null) {
            throw new TableNotFoundException(1);
        }

        return newEntries.entrySet().stream()
                .map(e -> table.update(context, fieldName, e.getKey(), e.getValue()))
                .reduce(0, Integer::sum);
    }

    /**
     * 读取一张表的数据
     *
     * @param context 事务context
     * @param tableName 表名
     * @param fieldName 字段名
     * @param uuids 待查询的uuid的列表
     * @return 查询到的行的Entry
     * @throws TableNotFoundException 不存在该表
     */
    List<Entry> read(TransactionContext context, String tableName, String fieldName, List<UUID> uuids) throws TableNotFoundException {
        var table = tableCache.get(tableName);
        if (table == null) {
            throw new TableNotFoundException(1);
        }

        return table.read(context, fieldName, uuids, recordStorage);
    }

    /**
     * 查询满足 [left, right) 区间uuid列表
     * 如果 left 为空，则查询 [ begin, right )
     * 如果 right 为空，则查询 [ left, end )
     * 如果 left, right 都为空，查询 [ begin, end )
     *
     * @param context 事务context
     * @param tableName 表名
     * @param fieldName 字段名
     * @param left 查询区间左端点
     * @param right 查询区间右端点
     * @return 查询到的uuid
     * @throws TableNotFoundException 不存在该表
     */
    List<UUID> search(TransactionContext context, String tableName, String fieldName, FieldValue left, FieldValue right) throws TableNotFoundException {
        var table = tableCache.get(tableName);
        if (table == null) {
            throw new TableNotFoundException(1);
        }

        return table.search(fieldName, left, right);
    }
}
