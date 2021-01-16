package net.kaaass.rumbase.query;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.parse.SqlParser;
import net.kaaass.rumbase.parse.exception.SqlSyntaxException;
import net.kaaass.rumbase.parse.stmt.CreateTableStatement;
import net.kaaass.rumbase.query.exception.ArgumentException;
import net.kaaass.rumbase.table.Table;
import net.kaaass.rumbase.table.TableManager;
import net.kaaass.rumbase.table.exception.TableConflictException;
import net.kaaass.rumbase.table.exception.TableExistenceException;
import net.kaaass.rumbase.table.field.FieldType;
import net.kaaass.rumbase.table.field.VarcharField;
import net.kaaass.rumbase.transaction.TransactionContext;

@Slf4j
public class CreateTableExecutorTest extends TestCase {

    public void testCreate() throws SqlSyntaxException {
        var sql = "CREATE TABLE testCreate$Persons\n" +
                "(\n" +
                "Id_P int not null,\n" +
                "LastName varchar(255),\n" +
                "FirstName varchar(255) NOT NULL\n" +
                ")";
        // 解析
        var stmt = SqlParser.parseStatement(sql);
        assertTrue(stmt instanceof CreateTableStatement);
        // 执行
        var manager = new TableManager();
        var context = TransactionContext.empty();
        var exe = new CreateTableExecutor((CreateTableStatement) stmt, manager, context);
        try {
            exe.execute();
        } catch (TableExistenceException | TableConflictException | ArgumentException e) {
            log.error("Exception expected: ", e);
            fail();
        }
        // 确认结果
        Table table = null;
        try {
            table = manager.getTable("testCreate$Persons");
        } catch (TableExistenceException e) {
            log.error("Exception expected: ", e);
            fail();
        }
        assertNotNull(table);
        var fields = table.getFields();
        assertEquals(3, fields.size());

        assertEquals("Id_P", fields.get(0).getName());
        assertEquals(FieldType.INT, fields.get(0).getType());
        assertFalse(fields.get(0).isNullable());


        assertEquals("LastName", fields.get(1).getName());
        assertEquals(FieldType.VARCHAR, fields.get(1).getType());
        assertEquals(255, ((VarcharField)fields.get(1)).getLimit());
        assertTrue(fields.get(1).isNullable());


        assertEquals("FirstName", fields.get(2).getName());
        assertEquals(FieldType.VARCHAR, fields.get(2).getType());
        assertEquals(255, ((VarcharField)fields.get(1)).getLimit());
        assertFalse(fields.get(2).isNullable());

    }
}
