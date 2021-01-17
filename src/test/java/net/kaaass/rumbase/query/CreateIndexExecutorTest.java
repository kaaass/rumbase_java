package net.kaaass.rumbase.query;

import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.FileUtil;
import net.kaaass.rumbase.index.exception.IndexAlreadyExistException;
import net.kaaass.rumbase.parse.SqlParser;
import net.kaaass.rumbase.parse.exception.SqlSyntaxException;
import net.kaaass.rumbase.parse.stmt.CreateIndexStatement;
import net.kaaass.rumbase.query.exception.ArgumentException;
import net.kaaass.rumbase.record.exception.RecordNotFoundException;
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

import java.util.ArrayList;

@Slf4j
public class CreateIndexExecutorTest {

    @BeforeClass
    @AfterClass
    public static void clearDataFolder() {
        log.info("清除数据文件夹...");
        FileUtil.removeDir(FileUtil.DATA_PATH);
    }

    @Test
    public void testParseSingle() throws SqlSyntaxException, IndexAlreadyExistException, TableExistenceException, TableConflictException, RecordNotFoundException, ArgumentException {
        var sql = "CREATE INDEX PersonIndex ON testParseSingle$Person (LastName) ;";
        // 解析
        var stmt = SqlParser.parseStatement(sql);
        Assert.assertTrue(stmt instanceof CreateIndexStatement);
        // 准备预期结果
        var manager = new TableManager();
        var context = TransactionContext.empty();
        var fields = new ArrayList<BaseField>();
        fields.add(new VarcharField("LastName", 20, false, null));
        try {
            manager.createTable(context, "testParseSingle$Person", fields, FileUtil.TABLE_PATH + "testParseSingle.Person.db");
        } catch (TableExistenceException | RecordNotFoundException | ArgumentException | TableConflictException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }
        try {
            var table = manager.getTable("testParseSingle$Person");
            var field = table.getField("LastName");
            Assert.assertTrue(field.isPresent());
            Assert.assertFalse(field.get().indexed());

            var createExe = new CreateIndexExecutor((CreateIndexStatement) stmt, manager, context);
            createExe.execute();

            Assert.assertTrue(field.get().indexed());
            Assert.assertEquals("data/index/testParseSingle$Person$LastName", field.get().getIndexName());
        } catch (TableExistenceException | IndexAlreadyExistException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }
    }

    @Test
    public void testParseMulti() throws SqlSyntaxException, IndexAlreadyExistException, TableExistenceException, TableConflictException, RecordNotFoundException, ArgumentException {
        var sql = "CREATE INDEX PersonIndex ON testParseMulti$Person (LastName, ID) ;";
        // 解析
        var stmt = SqlParser.parseStatement(sql);
        Assert.assertTrue(stmt instanceof CreateIndexStatement);
        // 准备预期结果
        var manager = new TableManager();
        var context = TransactionContext.empty();
        var fields = new ArrayList<BaseField>();
        fields.add(new VarcharField("LastName", 20, false, null));
        fields.add(new IntField("ID", false, null));
        try {
            manager.createTable(context, "testParseMulti$Person", fields, FileUtil.TABLE_PATH + "testParseMulti.Person.db");
        } catch (TableExistenceException | RecordNotFoundException | ArgumentException | TableConflictException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }
        try {
            var table = manager.getTable("testParseMulti$Person");
            var field1 = table.getField("LastName");
            var field2 = table.getField("ID");
            Assert.assertTrue(field1.isPresent());
            Assert.assertFalse(field1.get().indexed());
            Assert.assertTrue(field2.isPresent());
            Assert.assertFalse(field2.get().indexed());

            var createExe = new CreateIndexExecutor((CreateIndexStatement) stmt, manager, context);
            createExe.execute();

            Assert.assertTrue(field1.get().indexed());
            Assert.assertFalse(field2.get().indexed());
            Assert.assertEquals("data/index/testParseMulti$Person$LastName", field1.get().getIndexName());
        } catch (TableExistenceException | IndexAlreadyExistException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }
    }
}
