package net.kaaass.rumbase.table;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.table.exception.TableExistenceException;
import net.kaaass.rumbase.table.field.BaseField;
import net.kaaass.rumbase.table.field.FloatField;
import net.kaaass.rumbase.table.field.IntField;
import net.kaaass.rumbase.table.field.VarcharField;
import net.kaaass.rumbase.transaction.TransactionContext;

import java.util.ArrayList;

/**
 * 表管理器的测试
 *
 * @author @KveinAxel
 * @see net.kaaass.rumbase.table.TableManager
 */
@Slf4j
public class TableManagerTest extends TestCase {

    public void testShowTables() {
        var prefix = "testShowTables";

        var tbm = new TableManager();

        var fieldList = new ArrayList<BaseField>();
        var table = new Table(prefix + "Table", fieldList);

        // 增加测试表字段
        var intField = new IntField(prefix + "age", false, table);
        var floatField = new FloatField(prefix + "balance", false, table);
        var varcharField = new VarcharField(prefix + "name", 20, false, table);
        fieldList.add(intField);
        fieldList.add(floatField);
        fieldList.add(varcharField);

        try {
            tbm.createTable(TransactionContext.empty(), prefix + "Table", fieldList, prefix + ".db");
        } catch (TableExistenceException e) {
            e.printStackTrace();
            fail();
        }

        var tables = tbm.showTables();
        assertEquals(1, tables.size());
        assertEquals(prefix + "Table", tables.get(0));
    }

    public void testCreateTable() {
        var prefix = "testCreateTable";

        var tbm = new TableManager();

        var fieldList = new ArrayList<BaseField>();
        var table = new Table(prefix + "Table", fieldList);

        // 增加测试表字段
        var intField = new IntField(prefix + "age", false, table);
        var floatField = new FloatField(prefix + "balance", false, table);
        var varcharField = new VarcharField(prefix + "name", 20, false, table);
        fieldList.add(intField);
        fieldList.add(floatField);
        fieldList.add(varcharField);

        try {
            tbm.createTable(TransactionContext.empty(), prefix + "Table", fieldList, prefix + ".db");
        } catch (TableExistenceException e) {
            e.printStackTrace();
            fail();
        }

    }

    public void testGetTable() {
        var prefix = "testGetTable";

        var tbm = new TableManager();

        var fieldList = new ArrayList<BaseField>();
        var table = new Table(prefix + "Table", fieldList);

        // 增加测试表字段
        var intField = new IntField(prefix + "age", false, table);
        var floatField = new FloatField(prefix + "balance", false, table);
        var varcharField = new VarcharField(prefix + "name", 20, false, table);
        fieldList.add(intField);
        fieldList.add(floatField);
        fieldList.add(varcharField);

        try {
            tbm.createTable(TransactionContext.empty(), prefix + "Table", fieldList, prefix + ".db");
        } catch (TableExistenceException e) {
            e.printStackTrace();
            fail();
        }

        try {
            var t = tbm.getTable(prefix + "Table");
            assertEquals(prefix + "Table", t.tableName);
            assertEquals(intField.getName(), t.fields.get(0).getName());
            assertEquals(intField.getType(), t.fields.get(0).getType());
            assertEquals(floatField.getName(), t.fields.get(1).getName());
            assertEquals(floatField.getType(), t.fields.get(1).getType());
            assertEquals(varcharField.getName(), t.fields.get(2).getName());
            assertEquals(varcharField.getType(), t.fields.get(2).getType());
            assertEquals(varcharField.getLimit(), ((VarcharField) t.fields.get(2)).getLimit());

        } catch (TableExistenceException e) {
            fail();
        }
    }

}
