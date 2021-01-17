package net.kaaass.rumbase.query;

import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.FileUtil;
import net.kaaass.rumbase.index.exception.IndexAlreadyExistException;
import net.kaaass.rumbase.parse.SqlParser;
import net.kaaass.rumbase.parse.exception.SqlSyntaxException;
import net.kaaass.rumbase.parse.stmt.UpdateStatement;
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
public class UpdateExecutorTest {

    @BeforeClass
    @AfterClass
    public static void clearDataFolder() {
        log.info("清除数据文件夹...");
        FileUtil.removeDir(new File(FileUtil.DATA_PATH));
    }

    @Test
    public void testUpdateWithCondition() throws SqlSyntaxException, IndexAlreadyExistException, TableExistenceException, TableConflictException, RecordNotFoundException, ArgumentException {
        var sql = "UPDATE testUpdateWithCondition$Person SET Address = 'Zhongshan 23', City = 'Nanjing'\n" +
                "WHERE ID = 2";
        // 解析
        var stmt = SqlParser.parseStatement(sql);
        Assert.assertTrue(stmt instanceof UpdateStatement);

        var manager = new TableManager();
        var context = TransactionContext.empty();
        var fields = new ArrayList<BaseField>();
        var dummy = new Table("testUpdateWithCondition.__reserved__", fields);
        var lastName = new IntField("ID", false, dummy);
        fields.add(lastName);
        fields.add(new VarcharField("City", 20, false, dummy));
        fields.add(new VarcharField("Address", 20, false, dummy));
        Table table = null;
        try {
            manager.createTable(context, "testUpdateWithCondition$Person", fields, FileUtil.TABLE_PATH + "testUpdateWithCondition.Person.db");
            lastName.createIndex();
            table = manager.getTable("testUpdateWithCondition$Person");
        } catch (TableExistenceException | IndexAlreadyExistException | RecordNotFoundException | ArgumentException | TableConflictException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

        Assert.assertNotNull(table);
        try {
            table.insert(context, new ArrayList<>() {{
                add(0, "1");
                add(1, "'WenZhou'");
                add(2, "'Zhongshan 78'");
            }});
            table.insert(context, new ArrayList<>() {{
                add(0, "2");
                add(1, "'JiaXing'");
                add(2, "'Zhongshan 45'");
            }});

        } catch (TableConflictException | TableExistenceException | ArgumentException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

        // 测试插入结果
        try {
            var data = table.readAll(context);
            Assert.assertEquals(2, data.size());

            Assert.assertEquals(3, data.get(0).size());
            Assert.assertEquals(1, data.get(0).get(0));
            Assert.assertEquals("WenZhou", data.get(0).get(1));
            Assert.assertEquals("Zhongshan 78", data.get(0).get(2));

            Assert.assertEquals(3, data.get(1).size());
            Assert.assertEquals(2, data.get(1).get(0));
            Assert.assertEquals("JiaXing", data.get(1).get(1));
            Assert.assertEquals("Zhongshan 45", data.get(1).get(2));


        } catch (TableExistenceException | TableConflictException | ArgumentException | RecordNotFoundException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

        // 执行
        var exe = new UpdateExecutor((UpdateStatement) stmt, manager, context);
        try {
            exe.execute();
        } catch (TableExistenceException | ArgumentException | IndexAlreadyExistException | TableConflictException | RecordNotFoundException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

        // 检查执行结果
        try {
            var data = table.readAll(context);
            log.info("Result: {}", data);
            Assert.assertEquals(2, data.size());

            Assert.assertEquals(3, data.get(1).size());
            Assert.assertEquals(2, data.get(1).get(0));
            Assert.assertEquals("Nanjing", data.get(1).get(1));
            Assert.assertEquals("Zhongshan 23", data.get(1).get(2));

            Assert.assertEquals(3, data.get(0).size());
            Assert.assertEquals(1, data.get(0).get(0));
            Assert.assertEquals("WenZhou", data.get(0).get(1));
            Assert.assertEquals("Zhongshan 78", data.get(0).get(2));

        } catch (TableExistenceException | TableConflictException | ArgumentException | RecordNotFoundException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }
    }

    @Test
    public void testUpdateWithoutCondition() throws SqlSyntaxException, IndexAlreadyExistException, TableExistenceException, TableConflictException, RecordNotFoundException, ArgumentException {
        var sql = "UPDATE testUpdateWithoutCondition$Person SET Address = 'Zhongshan 23', City = 'Nanjing'";
        // 解析
        var stmt = SqlParser.parseStatement(sql);


        var manager = new TableManager();
        var context = TransactionContext.empty();
        var fields = new ArrayList<BaseField>();
        var lastName = new IntField("ID", false, null);
        fields.add(lastName);
        fields.add(new VarcharField("City", 20, false, null));
        fields.add(new VarcharField("Address", 20, false, null));
        Table table = null;
        try {
            manager.createTable(context, "testUpdateWithoutCondition$Person", fields, FileUtil.TABLE_PATH + "testUpdateWithoutCondition.Person.db");
            lastName.createIndex();
            table = manager.getTable("testUpdateWithoutCondition$Person");
        } catch (TableExistenceException | IndexAlreadyExistException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

        Assert.assertNotNull(table);
        try {
            table.insert(context, new ArrayList<>() {{
                add(0, "1");
                add(1, "'JiaXing'");
                add(2, "'Zhongshan 45'");
            }});
            table.insert(context, new ArrayList<>() {{
                add(0, "2");
                add(1, "'WenZhou'");
                add(2, "'Zhongshan 78'");
            }});

        } catch (TableConflictException | TableExistenceException | ArgumentException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

        // 测试插入结果
        try {
            var data = table.readAll(context);
            Assert.assertEquals(2, data.size());

            Assert.assertEquals(3, data.get(0).size());
            Assert.assertEquals(1, data.get(0).get(0));
            Assert.assertEquals("JiaXing", data.get(0).get(1));
            Assert.assertEquals("Zhongshan 45", data.get(0).get(2));

            Assert.assertEquals(3, data.get(1).size());
            Assert.assertEquals(2, data.get(1).get(0));
            Assert.assertEquals("WenZhou", data.get(1).get(1));
            Assert.assertEquals("Zhongshan 78", data.get(1).get(2));

        } catch (TableExistenceException | TableConflictException | ArgumentException | RecordNotFoundException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

        // 执行
        var exe = new UpdateExecutor((UpdateStatement) stmt, manager, context);
        try {
            exe.execute();
        } catch (TableExistenceException | ArgumentException | IndexAlreadyExistException | TableConflictException | RecordNotFoundException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

        // 检查执行结果
        try {
            var data = table.readAll(context);
            log.info("Result: {}", data);
            Assert.assertEquals(2, data.size());

            Assert.assertEquals(3, data.get(0).size());
            Assert.assertEquals(1, data.get(0).get(0));
            Assert.assertEquals("Nanjing", data.get(0).get(1));
            Assert.assertEquals("Zhongshan 23", data.get(0).get(2));

            Assert.assertEquals(3, data.get(1).size());
            Assert.assertEquals(2, data.get(1).get(0));
            Assert.assertEquals("Nanjing", data.get(1).get(1));
            Assert.assertEquals("Zhongshan 23", data.get(1).get(2));

        } catch (TableExistenceException | TableConflictException | ArgumentException | RecordNotFoundException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }
    }
}
