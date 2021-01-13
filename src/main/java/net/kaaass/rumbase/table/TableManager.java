package net.kaaass.rumbase.table;

import lombok.NoArgsConstructor;
import net.kaaass.rumbase.table.field.BaseField;
import net.kaaass.rumbase.table.exception.TableExistException;
import net.kaaass.rumbase.table.exception.TableNotFoundException;
import net.kaaass.rumbase.table.exception.TableConflictException;
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
     * @param isRepeatableRead 是否可重复读
     * @return 事务context
     */
    TransactionContext begin(boolean isRepeatableRead) {
        return TransactionContext.empty();
    }

    /**
     * 提交一个事务
     *
     * @param context 事务context
     */
    public void commit(TransactionContext context) {
        // todo commit tx
    }

    /**
     * 终止一个事务
     *
     * @param context 事务context
     */
    public void abort(TransactionContext context) {
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
     * @param baseFields    表的字段
     * @throws TableConflictException 该表已存在
     */
    public void createTable(
            TransactionContext context,
            String tableName,
            List<BaseField> baseFields
    ) throws TableExistException {
        if (tableCache.containsKey(tableName)) {
            throw new TableExistException(1);
        }

        var table = new Table(tableName, baseFields);
        table.create(context);

        tableCache.put(tableName, table);
    }

    public Table getTable(String tableName) throws TableNotFoundException {
        var table = tableCache.get(tableName);

        if (table == null) {
            throw new TableNotFoundException(1);
        } else {
            return table;
        }
    }

}
