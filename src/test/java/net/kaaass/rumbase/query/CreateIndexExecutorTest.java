package net.kaaass.rumbase.query;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.index.exception.IndexAlreadyExistException;
import net.kaaass.rumbase.parse.SqlParser;
import net.kaaass.rumbase.parse.exception.SqlSyntaxException;
import net.kaaass.rumbase.parse.stmt.CreateIndexStatement;
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
public class CreateIndexExecutorTest extends TestCase {

    public void testParseSingle() throws SqlSyntaxException, IndexAlreadyExistException, TableExistenceException, TableConflictException, RecordNotFoundException, ArgumentException {
        var sql = "CREATE INDEX PersonIndex ON testParseSingle$Person (LastName) ;";
        // 解析
        var stmt = SqlParser.parseStatement(sql);
        assertTrue(stmt instanceof CreateIndexStatement);
        // 准备预期结果
        var manager = new TableManager();
        var context = TransactionContext.empty();
        var fields = new ArrayList<BaseField>();
        fields.add(new VarcharField("LastName", 20, false, null));
        try {
            manager.createTable(context, "testParseSingle$Person", fields, "testParseSingle.Person.db");
        } catch (TableExistenceException | RecordNotFoundException | ArgumentException | TableConflictException e) {
            log.error("Exception expected: ", e);
            fail();
        }
        try {
            var table = manager.getTable("testParseSingle$Person");
            var field = table.getField("LastName");
            assertTrue(field.isPresent());
            assertFalse(field.get().indexed());

            var createExe = new CreateIndexExecutor((CreateIndexStatement) stmt, manager, context);
            createExe.execute();

            assertTrue(field.get().indexed());
            assertEquals("data/index/testParseSingle$Person$LastName", field.get().getIndexName());
        } catch (TableExistenceException | IndexAlreadyExistException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        new File("data/metadata.db").deleteOnExit();
        new File("data/metadata$key").deleteOnExit();
    }

    public void testParseMulti() throws SqlSyntaxException, IndexAlreadyExistException, TableExistenceException, TableConflictException, RecordNotFoundException, ArgumentException {
        var sql = "CREATE INDEX PersonIndex ON testParseMulti$Person (LastName, ID) ;";
        // 解析
        var stmt = SqlParser.parseStatement(sql);
        assertTrue(stmt instanceof CreateIndexStatement);
        // 准备预期结果
        var manager = new TableManager();
        var context = TransactionContext.empty();
        var fields = new ArrayList<BaseField>();
        fields.add(new VarcharField("LastName", 20, false, null));
        fields.add(new IntField("ID", false, null));
        try {
            manager.createTable(context, "testParseMulti$Person", fields, "testParseMulti.Person.db");
        } catch (TableExistenceException | RecordNotFoundException | ArgumentException | TableConflictException e) {
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

            var createExe = new CreateIndexExecutor((CreateIndexStatement) stmt, manager, context);
            createExe.execute();

            assertTrue(field1.get().indexed());
            assertFalse(field2.get().indexed());
            assertEquals("data/index/testParseMulti$Person$LastName", field1.get().getIndexName());
        } catch (TableExistenceException | IndexAlreadyExistException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        new File("data/metadata.db").deleteOnExit();
        new File("data/metadata$key").deleteOnExit();

    }
}
