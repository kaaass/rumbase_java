package net.kaaass.rumbase.query;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.index.exception.IndexAlreadyExistException;
import net.kaaass.rumbase.parse.SqlParser;
import net.kaaass.rumbase.parse.exception.SqlSyntaxException;
import net.kaaass.rumbase.parse.stmt.CreateIndexStatement;
import net.kaaass.rumbase.table.Table;
import net.kaaass.rumbase.table.TableManager;
import net.kaaass.rumbase.table.exception.TableExistenceException;
import net.kaaass.rumbase.table.field.BaseField;
import net.kaaass.rumbase.table.field.IntField;
import net.kaaass.rumbase.table.field.VarcharField;
import net.kaaass.rumbase.transaction.TransactionContext;

import java.util.ArrayList;

@Slf4j
public class CreateIndexExecutorTest extends TestCase {

    public void testParseSingle() throws SqlSyntaxException {
        var sql = "CREATE INDEX PersonIndex ON testParseSingle$Person (LastName) ;";
        // 解析
        var stmt = SqlParser.parseStatement(sql);
        assertTrue(stmt instanceof CreateIndexStatement);
        // 准备预期结果
        var manager = new TableManager();
        var context = TransactionContext.empty();
        var fields = new ArrayList<BaseField>();
        var dummy = new Table("testParseSingle.__reserved__", fields);
        fields.add(new VarcharField("LastName", 20, false, dummy));
        try {
            manager.createTable(context, "testParseSingle$Person", fields, "testParseSingle.Person.db");
        } catch (TableExistenceException e) {
            log.error("Exception expected: ", e);
            fail();
        }
        try {
            var table = manager.getTable("testParseSingle$Person");
            var field = table.getField("LastName");
            assertTrue(field.isPresent());
            assertFalse(field.get().indexed());

            var createExe = new CreateIndexExecutor((CreateIndexStatement) stmt, manager);
            createExe.execute();

            assertTrue(field.get().indexed());
            assertEquals("PersonIndex", field.get().getIndexName());
        } catch (TableExistenceException | IndexAlreadyExistException e) {
            log.error("Exception expected: ", e);
            fail();
        }


    }

    public void testParseMulti() throws SqlSyntaxException {
        var sql = "CREATE INDEX PersonIndex ON testParseMulti$Person (LastName, ID) ;";
        // 解析
        var stmt = SqlParser.parseStatement(sql);
        assertTrue(stmt instanceof CreateIndexStatement);
        // 准备预期结果
        var manager = new TableManager();
        var context = TransactionContext.empty();
        var fields = new ArrayList<BaseField>();
        var dummy = new Table("testParseMulti.__reserved__", fields);
        fields.add(new VarcharField("LastName", 20, false, dummy));
        fields.add(new IntField("ID", false, dummy));
        try {
            manager.createTable(context, "testParseMulti$Person", fields, "testParseMulti.Person.db");
        } catch (TableExistenceException e) {
            log.error("Exception expected: ", e);
            fail();
        }
        try {
            var table = manager.getTable("testParseMulti$Person");
            var field1 = table.getField("LastName");
            var field2 = table.getField("ID");
            assertTrue(field1.isPresent());
            assertFalse(field1.get().indexed());
            assertTrue(field2.isPresent());
            assertFalse(field2.get().indexed());

            var createExe = new CreateIndexExecutor((CreateIndexStatement) stmt, manager);
            createExe.execute();

            assertTrue(field1.get().indexed());
            assertFalse(field2.get().indexed());
            assertEquals("PersonIndex", field1.get().getIndexName());
        } catch (TableExistenceException | IndexAlreadyExistException e) {
            log.error("Exception expected: ", e);
            fail();
        }
    }
}
