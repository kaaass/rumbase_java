package net.kaaass.rumbase.query;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
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
import net.kaaass.rumbase.table.field.VarcharField;
import net.kaaass.rumbase.transaction.TransactionContext;

import java.io.File;
import java.util.ArrayList;

@Slf4j
public class UpdateExecutorTest extends TestCase {

    public void testUpdateWithCondition() throws SqlSyntaxException, IndexAlreadyExistException, TableExistenceException, TableConflictException, RecordNotFoundException, ArgumentException {
        var sql = "UPDATE testUpdateWithCondition$Person SET Address = 'Zhongshan 23', City = 'Nanjing'\n" +
                "WHERE LastName = 'Wilson'";
        // 解析
        var stmt = SqlParser.parseStatement(sql);
        assertTrue(stmt instanceof UpdateStatement);

        var manager = new TableManager();
        var context = TransactionContext.empty();
        var fields = new ArrayList<BaseField>();
        var dummy = new Table("testUpdateWithCondition.__reserved__", fields);
        var lastName = new VarcharField("LastName", 20, false, dummy);
        fields.add(lastName);
        fields.add(new VarcharField("City", 20, false, dummy));
        fields.add(new VarcharField("Address", 20, false, dummy));
        Table table = null;
        try {
            manager.createTable(context, "testUpdateWithCondition$Person", fields, "testUpdateWithCondition.Person.db");
            lastName.createIndex();
            table = manager.getTable("testUpdateWithCondition$Person");
        } catch (TableExistenceException | IndexAlreadyExistException | RecordNotFoundException | ArgumentException | TableConflictException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        assertNotNull(table);
        try {
            table.insert(context, new ArrayList<>() {{
                add(0, "'KAAAsS'");
                add(1, "'WenZhou'");
                add(2, "'Zhongshan 78'");
            }});
            table.insert(context, new ArrayList<>() {{
                add(0, "'Wilson'");
                add(1, "'JiaXing'");
                add(2, "'Zhongshan 45'");
            }});

        } catch (TableConflictException | TableExistenceException | ArgumentException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        // 测试插入结果
        try {
            var data = table.readAll(context);
            assertEquals(2, data.size());

            assertEquals(3, data.get(0).size());
            assertEquals("KAAAsS", (String) data.get(0).get(0));
            assertEquals("WenZhou", (String) data.get(0).get(1));
            assertEquals("Zhongshan 78", (String) data.get(0).get(2));

            assertEquals(3, data.get(1).size());
            assertEquals("Wilson", (String) data.get(1).get(0));
            assertEquals("JiaXing", (String) data.get(1).get(1));
            assertEquals("Zhongshan 45", (String) data.get(1).get(2));


        } catch (TableExistenceException | TableConflictException | ArgumentException | RecordNotFoundException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        // 执行
        var exe = new UpdateExecutor((UpdateStatement) stmt, manager, context);
        try {
            exe.execute();
        } catch (TableExistenceException | ArgumentException | IndexAlreadyExistException | TableConflictException | RecordNotFoundException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        // 检查执行结果
        try {
            var data = table.readAll(context);
            assertEquals(2, data.size());

            assertEquals(3, data.get(0).size());
            assertEquals("Wilson", (String) data.get(0).get(0));
            assertEquals("Nanjing", (String) data.get(0).get(1));
            assertEquals("Zhongshan 23", (String) data.get(0).get(2));

            assertEquals(3, data.get(1).size());
            assertEquals("KAAAsS", (String) data.get(1).get(0));
            assertEquals("WenZhou", (String) data.get(1).get(1));
            assertEquals("Zhongshan 78", (String) data.get(1).get(2));

        } catch (TableExistenceException | TableConflictException | ArgumentException | RecordNotFoundException e) {
            log.error("Exception expected: ", e);
            fail();
        }
        new File("data/metadata.db").deleteOnExit();
        new File("data/metadata$key").deleteOnExit();


    }

    public void testUpdateWithoutCondition() throws SqlSyntaxException, IndexAlreadyExistException, TableExistenceException, TableConflictException, RecordNotFoundException, ArgumentException {
        var sql = "UPDATE testUpdateWithoutCondition$Person SET Address = 'Zhongshan 23', City = 'Nanjing'";
        // 解析
        var stmt = SqlParser.parseStatement(sql);


        var manager = new TableManager();
        var context = TransactionContext.empty();
        var fields = new ArrayList<BaseField>();
        var dummy = new Table("testUpdateWithoutCondition.__reserved__", fields);
        var lastName = new VarcharField("LastName", 20, false, dummy);
        fields.add(lastName);
        fields.add(new VarcharField("City", 20, false, dummy));
        fields.add(new VarcharField("Address", 20, false, dummy));
        Table table = null;
        try {
            manager.createTable(context, "testUpdateWithoutCondition$Person", fields, "testUpdateWithoutCondition.Person.db");
            lastName.createIndex();
            table = manager.getTable("testUpdateWithoutCondition$Person");
        } catch (TableExistenceException | IndexAlreadyExistException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        assertNotNull(table);
        try {
            table.insert(context, new ArrayList<>() {{
                add(0, "'Wilson'");
                add(1, "'JiaXing'");
                add(2, "'Zhongshan 45'");
            }});
            table.insert(context, new ArrayList<>() {{
                add(0, "'KAAAsS'");
                add(1, "'WenZhou'");
                add(2, "'Zhongshan 78'");
            }});

        } catch (TableConflictException | TableExistenceException | ArgumentException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        // 测试插入结果
        try {
            var data = table.readAll(context);
            assertEquals(2, data.size());

            assertEquals(3, data.get(0).size());
            assertEquals("Wilson", (String) data.get(0).get(0));
            assertEquals("JiaXing", (String) data.get(0).get(1));
            assertEquals("Zhongshan 45", (String) data.get(0).get(2));

            assertEquals(3, data.get(1).size());
            assertEquals("KAAAsS", (String) data.get(1).get(0));
            assertEquals("WenZhou", (String) data.get(1).get(1));
            assertEquals("Zhongshan 78", (String) data.get(1).get(2));

        } catch (TableExistenceException | TableConflictException | ArgumentException | RecordNotFoundException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        // 执行
        var exe = new UpdateExecutor((UpdateStatement) stmt, manager, context);
        try {
            exe.execute();
        } catch (TableExistenceException | ArgumentException | IndexAlreadyExistException | TableConflictException | RecordNotFoundException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        // 检查执行结果
        try {
            var data = table.readAll(context);
            assertEquals(2, data.size());

            assertEquals(3, data.get(0).size());
            assertEquals("KAAAsS", (String) data.get(0).get(0));
            assertEquals("Nanjing", (String) data.get(0).get(1));
            assertEquals("Zhongshan 23", (String) data.get(0).get(2));

            assertEquals(3, data.get(1).size());
            assertEquals("Wilson", (String) data.get(1).get(0));
            assertEquals("Nanjing", (String) data.get(1).get(1));
            assertEquals("Zhongshan 23", (String) data.get(1).get(2));

        } catch (TableExistenceException | TableConflictException | ArgumentException | RecordNotFoundException e) {
            log.error("Exception expected: ", e);
            fail();
        }
        new File("data/metadata.db").deleteOnExit();
        new File("data/metadata.db").deleteOnExit();

    }
}
