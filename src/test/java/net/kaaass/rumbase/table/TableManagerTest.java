package net.kaaass.rumbase.table;

import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.FileUtil;
import net.kaaass.rumbase.index.exception.IndexAlreadyExistException;
import net.kaaass.rumbase.query.exception.ArgumentException;
import net.kaaass.rumbase.record.exception.RecordNotFoundException;
import net.kaaass.rumbase.table.exception.TableConflictException;
import net.kaaass.rumbase.table.exception.TableExistenceException;
import net.kaaass.rumbase.table.field.BaseField;
import net.kaaass.rumbase.table.field.FloatField;
import net.kaaass.rumbase.table.field.IntField;
import net.kaaass.rumbase.table.field.VarcharField;
import net.kaaass.rumbase.transaction.TransactionContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;

/**
 * 表管理器的测试
 *
 * @author @KveinAxel
 * @see net.kaaass.rumbase.table.TableManager
 */
@Slf4j
public class TableManagerTest {

    @BeforeClass
    @AfterClass
    public static void clearDataFolder() {
        log.info("清除数据文件夹...");
        FileUtil.removeDir(new File(FileUtil.DATA_PATH));
    }

    @Test
    public void testShowTables() throws IndexAlreadyExistException, TableExistenceException, TableConflictException, RecordNotFoundException, ArgumentException {
        var prefix = "testShowTables";

        var tbm = new TableManager();

        var fieldList = new ArrayList<BaseField>();

        // 增加测试表字段
        var intField = new IntField(prefix + "age", false, null);
        var floatField = new FloatField(prefix + "balance", false, null);
        var varcharField = new VarcharField(prefix + "name", 20, false, null);
        fieldList.add(intField);
        fieldList.add(floatField);
        fieldList.add(varcharField);

        try {
            tbm.createTable(TransactionContext.empty(), prefix + "Table", fieldList, FileUtil.PATH + prefix + ".db");
        } catch (TableExistenceException | RecordNotFoundException | ArgumentException | TableConflictException e) {
            e.printStackTrace();
            Assert.fail();
        }

        var tables = tbm.showTables();
        Assert.assertEquals(4, tables.size());
        Assert.assertEquals(prefix + "Table", tables.get(0));
    }

    @Test
    public void testCreateTable() throws IndexAlreadyExistException, TableExistenceException, TableConflictException, RecordNotFoundException, ArgumentException {
        var prefix = "testCreateTable";

        var tbm = new TableManager();

        var fieldList = new ArrayList<BaseField>();

        // 增加测试表字段
        var intField = new IntField(prefix + "age", false, null);
        var floatField = new FloatField(prefix + "balance", false, null);
        var varcharField = new VarcharField(prefix + "name", 20, false, null);
        fieldList.add(intField);
        fieldList.add(floatField);
        fieldList.add(varcharField);

        try {
            tbm.createTable(TransactionContext.empty(), prefix + "Table", fieldList, FileUtil.PATH + prefix + ".db");
        } catch (TableExistenceException e) {
            e.printStackTrace();
            Assert.fail();
        }
    }

    @Test
    public void testGetTable() throws IndexAlreadyExistException, TableExistenceException, TableConflictException, RecordNotFoundException, ArgumentException {
        var prefix = "testGetTable";

        var tbm = new TableManager();

        var fieldList = new ArrayList<BaseField>();

        // 增加测试表字段
        var intField = new IntField(prefix + "age", false, null);
        var floatField = new FloatField(prefix + "balance", false, null);
        var varcharField = new VarcharField(prefix + "name", 20, false, null);
        fieldList.add(intField);
        fieldList.add(floatField);
        fieldList.add(varcharField);

        try {
            tbm.createTable(TransactionContext.empty(), prefix + "Table", fieldList, FileUtil.PATH + prefix + ".db");
        } catch (TableExistenceException e) {
            e.printStackTrace();
            Assert.fail();
        }

        try {
            var t = tbm.getTable(prefix + "Table");
            Assert.assertEquals(prefix + "Table", t.tableName);
            Assert.assertEquals(intField.getName(), t.fields.get(0).getName());
            Assert.assertEquals(intField.getType(), t.fields.get(0).getType());
            Assert.assertEquals(floatField.getName(), t.fields.get(1).getName());
            Assert.assertEquals(floatField.getType(), t.fields.get(1).getType());
            Assert.assertEquals(varcharField.getName(), t.fields.get(2).getName());
            Assert.assertEquals(varcharField.getType(), t.fields.get(2).getType());
            Assert.assertEquals(varcharField.getLimit(), ((VarcharField) t.fields.get(2)).getLimit());

        } catch (TableExistenceException e) {
            Assert.fail();
        }
    }

}
