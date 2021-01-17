package net.kaaass.rumbase.query;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.FileUtil;
import net.kaaass.rumbase.index.exception.IndexAlreadyExistException;
import net.kaaass.rumbase.parse.SqlParser;
import net.kaaass.rumbase.parse.exception.SqlSyntaxException;
import net.kaaass.rumbase.parse.stmt.CreateTableStatement;
import net.kaaass.rumbase.query.exception.ArgumentException;
import net.kaaass.rumbase.record.exception.RecordNotFoundException;
import net.kaaass.rumbase.table.Table;
import net.kaaass.rumbase.table.TableManager;
import net.kaaass.rumbase.table.exception.TableConflictException;
import net.kaaass.rumbase.table.exception.TableExistenceException;
import net.kaaass.rumbase.table.field.FieldType;
import net.kaaass.rumbase.table.field.VarcharField;
import net.kaaass.rumbase.transaction.TransactionContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;

@Slf4j
public class CreateTableExecutorTest {

    @BeforeClass
    @AfterClass
    public static void clearDataFolder() {
        log.info("清除数据文件夹...");
        FileUtil.removeDir(new File(FileUtil.DATA_PATH));
    }

    @Test
    public void testCreate() throws SqlSyntaxException, IndexAlreadyExistException, TableExistenceException, TableConflictException, RecordNotFoundException, ArgumentException {
        var sql = "CREATE TABLE testCreate$Persons\n" +
                "(\n" +
                "Id_P int not null,\n" +
                "LastName varchar(255),\n" +
                "FirstName varchar(255) NOT NULL\n" +
                ")";
        // 解析
        var stmt = SqlParser.parseStatement(sql);
        Assert.assertTrue(stmt instanceof CreateTableStatement);
        // 执行
        var manager = new TableManager();
        var context = TransactionContext.empty();
        var exe = new CreateTableExecutor((CreateTableStatement) stmt, manager, context);
        try {
            exe.execute();
        } catch (TableExistenceException | TableConflictException | ArgumentException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }
        // 确认结果
        Table table = null;
        try {
            table = manager.getTable("testCreate$Persons");
        } catch (TableExistenceException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }
        Assert.assertNotNull(table);
        var fields = table.getFields();
        Assert.assertEquals(3, fields.size());

        Assert.assertEquals("Id_P", fields.get(0).getName());
        Assert.assertEquals(FieldType.INT, fields.get(0).getType());
        Assert.assertFalse(fields.get(0).isNullable());


        Assert.assertEquals("LastName", fields.get(1).getName());
        Assert.assertEquals(FieldType.VARCHAR, fields.get(1).getType());
        Assert.assertEquals(255, ((VarcharField) fields.get(1)).getLimit());
        Assert.assertTrue(fields.get(1).isNullable());


        Assert.assertEquals("FirstName", fields.get(2).getName());
        Assert.assertEquals(FieldType.VARCHAR, fields.get(2).getType());
        Assert.assertEquals(255, ((VarcharField) fields.get(1)).getLimit());
        Assert.assertFalse(fields.get(2).isNullable());
    }
}
