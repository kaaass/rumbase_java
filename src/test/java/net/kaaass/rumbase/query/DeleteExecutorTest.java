package net.kaaass.rumbase.query;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.index.exception.IndexAlreadyExistException;
import net.kaaass.rumbase.parse.SqlParser;
import net.kaaass.rumbase.parse.exception.SqlSyntaxException;
import net.kaaass.rumbase.parse.stmt.DeleteStatement;
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

import java.io.File;
import java.util.ArrayList;

@Slf4j
public class DeleteExecutorTest extends TestCase {

    public void testDelete() throws SqlSyntaxException, IndexAlreadyExistException, TableExistenceException, TableConflictException, RecordNotFoundException, ArgumentException {
        var sql = "DELETE FROM testDelete$Person WHERE LastName = 'KevinAxel'";
        // 解析
        var stmt = SqlParser.parseStatement(sql);
        assertTrue(stmt instanceof DeleteStatement);

        var manager = new TableManager();
        var context = TransactionContext.empty();
        var fields = new ArrayList<BaseField>();
        var id = new IntField("ID", false, null);
        fields.add(new VarcharField("LastName", 20, false, null));
        fields.add(id);
        Table table = null;
        try {
            manager.createTable(context, "testDelete$Person", fields, "data/table/testDelete.Person.db");
            id.createIndex();
            table = manager.getTable("testDelete$Person");
        } catch (TableExistenceException | IndexAlreadyExistException | RecordNotFoundException | ArgumentException | TableConflictException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        assertNotNull(table);
        try {
            table.insert(context, new ArrayList<>() {{
                add(0, "'KevinAxel'");
                add(1, "1");
            }});
            table.insert(context, new ArrayList<>() {{
                add(0, "'KAAAsS'");
                add(1, "2");
            }});

        } catch (TableConflictException | TableExistenceException | ArgumentException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        // 测试插入结果
        try {
            var data = table.readAll(context);
            assertEquals(2, data.size());

            assertEquals(2, data.get(0).size());
            assertEquals("KevinAxel", (String) data.get(0).get(0));
            assertEquals(1, (int) data.get(0).get(1));

            assertEquals(2, data.get(1).size());
            assertEquals("KAAAsS", (String) data.get(1).get(0));
            assertEquals(2, (int) data.get(1).get(1));

        } catch (TableExistenceException | TableConflictException | ArgumentException | RecordNotFoundException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        // 执行
        var exe = new DeleteExecutor((DeleteStatement) stmt, manager, context);
        try {
            exe.execute();
        } catch (TableExistenceException | ArgumentException | IndexAlreadyExistException | TableConflictException | RecordNotFoundException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        // 测试执行结果
        try {
            var data = table.readAll(context);
            assertEquals(1, data.size());

            assertEquals(2, data.get(0).size());
            assertEquals("KAAAsS", (String) data.get(0).get(0));
            assertEquals(2, (int) data.get(0).get(1));

        } catch (TableExistenceException | TableConflictException | ArgumentException | RecordNotFoundException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        new File("data/metadata.db").deleteOnExit();
        new File("data/metadata$key").deleteOnExit();


    }

    public void testDeleteAll() throws SqlSyntaxException, IndexAlreadyExistException, TableExistenceException, TableConflictException, RecordNotFoundException, ArgumentException {
        var sql = "DELETE FROM testDeleteAll$Person ";
        // 解析
        var stmt = SqlParser.parseStatement(sql);
        assertTrue(stmt instanceof DeleteStatement);

        var manager = new TableManager();
        var context = TransactionContext.empty();
        var fields = new ArrayList<BaseField>();
        var id = new IntField("ID", false, null);
        fields.add(new VarcharField("LastName", 20, false, null));
        fields.add(id);
        Table table = null;
        try {
            manager.createTable(context, "testDeleteAll$Person", fields, "data/table/testDeleteAll.Person.db");
            id.createIndex();
            table = manager.getTable("testDeleteAll$Person");
        } catch (TableExistenceException | IndexAlreadyExistException | RecordNotFoundException | ArgumentException | TableConflictException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        assertNotNull(table);
        try {
            table.insert(context, new ArrayList<>() {{
                add(0, "'KevinAxel'");
                add(1, "1");
            }});
            table.insert(context, new ArrayList<>() {{
                add(0, "'KAAAsS'");
                add(1, "2");
            }});

        } catch (TableConflictException | TableExistenceException | ArgumentException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        // 测试插入结果
        try {
            var data = table.readAll(context);
            assertEquals(2, data.size());

            assertEquals(2, data.get(0).size());
            assertEquals("KevinAxel", (String) data.get(0).get(0));
            assertEquals(1, (int) data.get(0).get(1));

            assertEquals(2, data.get(1).size());
            assertEquals("KAAAsS", (String) data.get(1).get(0));
            assertEquals(2, (int) data.get(1).get(1));

        } catch (TableExistenceException | TableConflictException | ArgumentException | RecordNotFoundException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        // 执行
        var exe = new DeleteExecutor((DeleteStatement) stmt, manager, context);
        try {
            exe.execute();
        } catch (TableExistenceException | ArgumentException | IndexAlreadyExistException | TableConflictException | RecordNotFoundException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        // 测试执行结果
        try {
            var data = table.readAll(context);
            assertEquals(0, data.size());

        } catch (TableExistenceException | TableConflictException | ArgumentException | RecordNotFoundException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        new File("data/metadata.db").deleteOnExit();
        new File("data/metadata$key").deleteOnExit();


    }
}
