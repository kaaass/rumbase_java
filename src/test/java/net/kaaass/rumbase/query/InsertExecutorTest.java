package net.kaaass.rumbase.query;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
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

import java.io.File;
import java.util.ArrayList;

@Slf4j
public class InsertExecutorTest extends TestCase {

    private static final String PATH = "build/";

    public void testInsertColumnValue() throws SqlSyntaxException {
        var sql = "INSERT INTO Persons (Persons.LastName, Address) VALUES ('Wilson', 'Champs-Elysees')";
        // 解析
        var stmt = SqlParser.parseStatement(sql);
        assertTrue(stmt instanceof InsertStatement);

        var manager = new TableManager();
        var context = TransactionContext.empty();
        var fields = new ArrayList<BaseField>();
        var lastName = new VarcharField("LastName", 20, false, null);
        fields.add(lastName);
        fields.add(new VarcharField("Address", 255, false, null));
        Table table = null;
        try {
            manager.createTable(context, "Persons", fields, PATH + "testInsertColumnValue.Persons.db");
            lastName.createIndex();
            table = manager.getTable("Persons");
        } catch (TableExistenceException | IndexAlreadyExistException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        assertNotNull(table);

        // 执行
        var exe = new InsertExecutor((InsertStatement) stmt, manager, context);
        try {
            exe.execute();
        } catch (TableExistenceException | TableConflictException | ArgumentException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        // 确认结果
        try {
            var data = table.readAll(context);
            assertEquals(1, data.size());
            assertEquals("Wilson", data.get(0).get(0));
            assertEquals("Champs-Elysees", data.get(0).get(1));
        } catch (TableExistenceException | TableConflictException | ArgumentException | RecordNotFoundException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        new File("metadata.db").deleteOnExit();

    }

    public void testInsertValue() throws SqlSyntaxException {
        var sql = "INSERT INTO stu VALUES (20200101, 'KAAAsS', true, 3.9)";
        // 解析
        var stmt = SqlParser.parseStatement(sql);
        assertTrue(stmt instanceof InsertStatement);

        var manager = new TableManager();
        var context = TransactionContext.empty();
        var fields = new ArrayList<BaseField>();
        var id = new IntField("ID", false, null);
        fields.add(new VarcharField("LastName", 20, false, null));
        fields.add(id);
        Table table = null;
        try {
            manager.createTable(context, "Person", fields, PATH + "testInsertValue.Person.db");
            id.createIndex();
            table = manager.getTable("Person");
        } catch (TableExistenceException | IndexAlreadyExistException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        assertNotNull(table);
        new File("metadata.db").deleteOnExit();

    }
}
