package net.kaaass.rumbase.table;

import com.igormaznitsa.jbbp.io.JBBPBitInputStream;
import com.igormaznitsa.jbbp.io.JBBPBitOutputStream;
import com.igormaznitsa.jbbp.io.JBBPByteOrder;
import lombok.Getter;
import net.kaaass.rumbase.index.exception.IndexNotFoundException;
import net.kaaass.rumbase.record.IRecordStorage;
import net.kaaass.rumbase.record.RecordManager;
import net.kaaass.rumbase.table.field.BaseField;
import net.kaaass.rumbase.table.exception.TableExistenceException;
import net.kaaass.rumbase.transaction.TransactionContext;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
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

public class TableManager {

    private final IRecordStorage metaRecord = RecordManager.fromFile("metadata.db");

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

    public TableManager() {
        load();
    }

    public void load() {
        var context = TransactionContext.empty();
        var meta = metaRecord.getMetadata(context);

        var byteInStream = new ByteArrayInputStream(meta);
        var stream = new JBBPBitInputStream(byteInStream);

        int num;
        try {
            num = stream.readInt(JBBPByteOrder.BIG_ENDIAN);
        } catch (IOException e) {
            return;
        }
        for (int i = 0; i < num; i++) {
            try {
                var key = stream.readString(JBBPByteOrder.BIG_ENDIAN);
                var val = stream.readString(JBBPByteOrder.BIG_ENDIAN);
                // 加载表
                if (key.startsWith("tablePath$")) {
                    var tableName = key.split("\\$")[1];
                    var record = RecordManager.fromFile(val);
                    recordPaths.add(val);
                    var table = Table.load(record);
                    tableCache.put(tableName, table);
                }
            } catch (IOException e) {
                e.printStackTrace();
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
    ) throws TableExistenceException {
        if (tableCache.containsKey(tableName)) {
            throw new TableExistenceException(1);
        }

        var table = new Table(tableName, baseFields, path);

        for (var f: baseFields) {
            f.setParentTable(table);
        }

        table.persist(context);

        var meta = metaRecord.getMetadata(TransactionContext.empty());

        var in = new ByteArrayInputStream(meta);
        var inStream = new JBBPBitInputStream(in);

        int cnt;
        try {
            cnt = inStream.readInt(JBBPByteOrder.BIG_ENDIAN);

        } catch (IOException e) {
            cnt = 0;
        }

        var byteOutStream = new ByteArrayOutputStream();
        var stream = new JBBPBitOutputStream(byteOutStream);
        try {
            stream.writeInt(cnt + 1, JBBPByteOrder.BIG_ENDIAN);
            stream.write(in.readAllBytes());
            stream.writeString("tablePath$" + tableName, JBBPByteOrder.BIG_ENDIAN);
            stream.writeString(path, JBBPByteOrder.BIG_ENDIAN);
        } catch (IOException e) {
            e.printStackTrace();
        }

        metaRecord.setMetadata(context, byteOutStream.toByteArray());
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
