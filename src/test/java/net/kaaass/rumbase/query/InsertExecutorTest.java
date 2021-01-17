package net.kaaass.rumbase.query;

import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.FileUtil;
import net.kaaass.rumbase.index.exception.IndexAlreadyExistException;
import net.kaaass.rumbase.parse.SqlParser;
import net.kaaass.rumbase.parse.exception.SqlSyntaxException;
import net.kaaass.rumbase.parse.stmt.InsertStatement;
import net.kaaass.rumbase.query.exception.ArgumentException;
import net.kaaass.rumbase.record.exception.RecordNotFoundException;
import net.kaaass.rumbase.table.Table;
import net.kaaass.rumbase.table.TableManager;
import net.kaaass.rumbase.table.exception.TableConflictException;
import net.kaaass.rumbase.table.exception.TableExistenceException;
import net.kaaass.rumbase.table.field.BaseField;
import net.kaaass.rumbase.table.field.IntField;
import net.kaaass.rumbase.table.field.VarcharField;
import net.kaaass.rumbase.transaction.TransactionContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;

@Slf4j
public class InsertExecutorTest {

    @BeforeClass
    @AfterClass
    public static void clearDataFolder() {
        log.info("清除数据文件夹...");
        FileUtil.removeDir(new File(FileUtil.DATA_PATH));
    }

    @Test
    public void testInsertColumnValue() throws SqlSyntaxException, IndexAlreadyExistException, TableExistenceException, TableConflictException, RecordNotFoundException, ArgumentException {
        var sql = "INSERT INTO Persons (Persons.LastName, Address) VALUES ('Wilson', 'Champs-Elysees')";
        // 解析
        var stmt = SqlParser.parseStatement(sql);
        Assert.assertTrue(stmt instanceof InsertStatement);

        var manager = new TableManager();
        var context = TransactionContext.empty();
        var fields = new ArrayList<BaseField>();
        var lastName = new VarcharField("LastName", 20, false, null);
        fields.add(lastName);
        fields.add(new VarcharField("Address", 255, false, null));
        Table table = null;
        try {
            manager.createTable(context, "Persons", fields, FileUtil.TABLE_PATH + "testInsertColumnValue.Persons.db");
            lastName.createIndex();
            table = manager.getTable("Persons");
        } catch (TableExistenceException | IndexAlreadyExistException | RecordNotFoundException | ArgumentException | TableConflictException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

        Assert.assertNotNull(table);

        // 执行
        var exe = new InsertExecutor((InsertStatement) stmt, manager, context);
        try {
            exe.execute();
        } catch (TableExistenceException | TableConflictException | ArgumentException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

        // 确认结果
        try {
            var data = table.readAll(context);
            Assert.assertEquals(1, data.size());
            Assert.assertEquals("Wilson", data.get(0).get(0));
            Assert.assertEquals("Champs-Elysees", data.get(0).get(1));
        } catch (TableExistenceException | TableConflictException | ArgumentException | RecordNotFoundException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }
    }

    @Test
    public void testInsertValue() throws SqlSyntaxException, IndexAlreadyExistException, TableExistenceException, TableConflictException, RecordNotFoundException, ArgumentException {
        var sql = "INSERT INTO stu VALUES (20200101, 'KAAAsS', true, 3.9)";
        // 解析
        var stmt = SqlParser.parseStatement(sql);
        Assert.assertTrue(stmt instanceof InsertStatement);

        var manager = new TableManager();
        var context = TransactionContext.empty();
        var fields = new ArrayList<BaseField>();
        var id = new IntField("ID", false, null);
        fields.add(new VarcharField("LastName", 20, false, null));
        fields.add(id);
        Table table = null;
        try {
            manager.createTable(context, "Person", fields, FileUtil.TABLE_PATH + "testInsertValue.Person.db");
            id.createIndex();
            table = manager.getTable("Person");
        } catch (TableExistenceException | IndexAlreadyExistException | RecordNotFoundException | ArgumentException | TableConflictException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

        Assert.assertNotNull(table);
    }
}
