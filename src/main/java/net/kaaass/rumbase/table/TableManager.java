package net.kaaass.rumbase.table;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.index.exception.IndexAlreadyExistException;
import net.kaaass.rumbase.query.exception.ArgumentException;
import net.kaaass.rumbase.record.IRecordStorage;
import net.kaaass.rumbase.record.RecordManager;
import net.kaaass.rumbase.record.exception.RecordNotFoundException;
import net.kaaass.rumbase.recovery.RecoveryManager;
import net.kaaass.rumbase.recovery.exception.LogException;
import net.kaaass.rumbase.table.exception.TableConflictException;
import net.kaaass.rumbase.table.field.BaseField;
import net.kaaass.rumbase.table.exception.TableExistenceException;
import net.kaaass.rumbase.table.field.VarcharField;
import net.kaaass.rumbase.transaction.TransactionContext;

import java.io.File;
import java.util.*;

/**
 * 表管理器
 * <p>
 * ITableManager用于管理表结构, 已经为上层模块提供更加高级和抽象的接口.
 * <p>
 * ITableManager会依赖index模块进行索引, 依赖record模块进行表单数据查找.
 * <p>
 *
 * @author @KveinAxel
 */
@Slf4j
public class TableManager {

    static {
        var dataDir = new File("data/");
        if (!dataDir.exists() && !dataDir.isDirectory()) {
            dataDir.mkdirs();
        }
    }

    private final IRecordStorage metaRecord = RecordManager.fromFile("data/metadata.db");

    /**
     * 对所有的表结构的缓存
     * <p>
     * 如果不在这里，说明就没有这张表
     * <p>
     * 同样地，创建表、删除表需要维护这个结构
     */
    private final Map<String, Table> tableCache = new HashMap<>();

    @Getter
    private final List<String > recordPaths = new ArrayList<>();


    /**
     * 提交一个事务
     *
     * @param context 事务context
     */
    public void commit(TransactionContext context) {
        context.commit();
    }

    /**
     * 终止一个事务
     *
     * @param context 事务context
     */
    public void abort(TransactionContext context) {
        context.rollback();
    }

    public TableManager() throws IndexAlreadyExistException {
        load();
    }

    public void load() throws IndexAlreadyExistException {
        var context = TransactionContext.empty();
        Table metaTable;
        try {
            metaTable = Table.load(metaRecord);
            tableCache.put("metadata", metaTable);
        } catch (RuntimeException e) {
            // 新建表
            var fields = new ArrayList<BaseField>();
            var keyField = new VarcharField("key", 255, false, null);
            var valueField = new VarcharField("value", 255, false, null);
            fields.add(keyField);
            fields.add(valueField);

            var dataDir = new File("data/");
            if (!dataDir.exists() && !dataDir.isDirectory()) {
                dataDir.mkdirs();
            }

            metaTable = new Table("metadata", fields, "data/metadata.db");

            for (var f: fields) {
                f.setParentTable(metaTable);
            }

            keyField.createIndex("data/");

            metaTable.persist(context);
            tableCache.put("metadata", metaTable);
        }
    }

    /**
     * 启动服务器前进行的准备
     */
    public void prepare() {
        var context = TransactionContext.empty();
        var metaTable = tableCache.get("metadata");
        List<List<Object>> data = null;
        try {
            data = metaTable.readAll(context);
        } catch (TableExistenceException | TableConflictException | ArgumentException | RecordNotFoundException e) {
            log.error("无法读入元数据表，数据可能损坏！", e);
            System.exit(1);
        }
        // 载入所有已有表
        var map = new HashMap<String, String>();
        data.forEach(row -> map.put((String) row.get(0), (String) row.get(1)));

        if (!map.containsKey("table_num")) {
            try {
                metaTable.insert(context, new ArrayList<>(){{
                    add("'table_num'");
                    add("'0'");
                }});
            } catch (TableConflictException | TableExistenceException | ArgumentException e) {
                log.error("无法初始化元数据表", e);
                System.exit(1);
            }
            map.put("table_num", "0");
        }

        for (var item: map.entrySet()) {
            if (item.getKey().startsWith("tablePath$")) {
                var tableName = item.getKey().split("\\$")[1];
                var tablePath = item.getValue();
                // 恢复表
                try {
                    RecoveryManager.recovery(tablePath);
                } catch (LogException e) {
                    log.error("无法恢复表 {} 于 {}，数据可能损坏！", tableName, tablePath, e);
                    System.exit(1);
                }
                // 读入表
                var record = RecordManager.fromFile(item.getValue());
                recordPaths.add(tablePath);
                var table = Table.load(record);
                tableCache.put(tableName, table);
            }
        }
    }

    /**
     * 显示所有的表名
     *
     * @return 表名的列表
     */
    public List<String> showTables() {
        List<String> list = new ArrayList<>();

        tableCache.forEach((k, v) -> list.add(k));

        return list;
    }

    /**
     * 创建一个表
     *
     * @param context   事务context
     * @param tableName 表名
     * @param baseFields    表的字段
     * @throws TableExistenceException 该表已存在
     */
    public void createTable(
            TransactionContext context,
            String tableName,
            List<BaseField> baseFields,
            String path
    ) throws TableExistenceException, TableConflictException, RecordNotFoundException, ArgumentException {
        if (tableCache.containsKey(tableName)) {
            throw new TableExistenceException(1);
        }

        var tableDir = new File("data/table/");
        if (!tableDir.exists() && !tableDir.isDirectory()) {
            tableDir.mkdirs();
        }

        var table = new Table(tableName, baseFields, path);

        for (var f: baseFields) {
            f.setParentTable(table);
        }

        table.persist(context);

        var metaTable = tableCache.get("metadata");

        var uuids = metaTable.search("key", "table_num");

        int cnt = -1;
        long cntUuid = -1;
        for (var uuid: uuids) {
            var res = metaTable.read(TransactionContext.empty(), uuid);
            if (res.isPresent()) {
                cnt = Integer.parseInt((String) res.get().get(1));
                cntUuid = uuid;
                break;
            }
        }

        if (cnt == -1 || cntUuid == -1 ) {
            throw new RuntimeException();
        }

        cnt = cnt + 1;
        var newCntEntry = new ArrayList<String>();
        newCntEntry.add("'table_num'");
        newCntEntry.add("'" + Integer.toString(cnt) + "'");
        metaTable.update(TransactionContext.empty(), cntUuid, newCntEntry);

        var newTableData = new ArrayList<String>();
        newTableData.add("'tablePath$" + tableName + "'");
        newTableData.add("'" + path + "'");

        metaTable.insert(TransactionContext.empty(), newTableData);

        tableCache.put(tableName, table);
    }

    public Table getTable(String tableName) throws TableExistenceException {
        var table = tableCache.get(tableName);

        if (table == null) {
            throw new TableExistenceException(1);
        } else {
            return table;
        }
    }

}
