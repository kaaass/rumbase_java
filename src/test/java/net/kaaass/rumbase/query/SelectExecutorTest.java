package net.kaaass.rumbase.query;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.index.exception.IndexAlreadyExistException;
import net.kaaass.rumbase.parse.SqlParser;
import net.kaaass.rumbase.parse.exception.SqlSyntaxException;
import net.kaaass.rumbase.parse.stmt.SelectStatement;
import net.kaaass.rumbase.query.exception.ArgumentException;
import net.kaaass.rumbase.record.exception.RecordNotFoundException;
import net.kaaass.rumbase.table.Table;
import net.kaaass.rumbase.table.TableManager;
import net.kaaass.rumbase.table.exception.TableConflictException;
import net.kaaass.rumbase.table.exception.TableExistenceException;
import net.kaaass.rumbase.table.field.BaseField;
import net.kaaass.rumbase.table.field.FloatField;
import net.kaaass.rumbase.table.field.IntField;
import net.kaaass.rumbase.table.field.VarcharField;
import net.kaaass.rumbase.transaction.TransactionContext;

import java.io.File;
import java.util.ArrayList;

@Slf4j
public class SelectExecutorTest extends TestCase {

    private static final String PATH = "build/";

    public void testSelect() throws SqlSyntaxException, IndexAlreadyExistException, TableExistenceException, TableConflictException, RecordNotFoundException, ArgumentException {
        var sql = "SELECT distinct name, testSelect$account.ID, testSelect$account.balance \n" +
                "from testSelect$account join testSelect$payment on testSelect$account.ID = testSelect$payment.ID\n" +
                "WHERE testSelect$account.ID > 1 and (testSelect$payment.type = 'N' or testSelect$payment.type = 'T') \n" +
                "order by testSelect$account.ID desc;";
        // 解析
        var stmt = SqlParser.parseStatement(sql);

        // 创建测试表
        var manager = new TableManager();
        var context = TransactionContext.empty();

        // 创建account表
        var accountFields = new ArrayList<BaseField>();
        var id = new IntField("ID", false, null);
        accountFields.add(id);
        accountFields.add(new VarcharField("name", 20, false, null));
        accountFields.add(new FloatField("balance", false, null));
        Table account = null;
        try {
            manager.createTable(context, "testSelect$account", accountFields, PATH + "testSelect.account.db");
            id.createIndex();
            account = manager.getTable("testSelect$account");
        } catch (TableExistenceException | IndexAlreadyExistException | RecordNotFoundException | ArgumentException | TableConflictException e) {
            log.error("Exception expected: ", e);
            fail();
        }
        assertNotNull(account);
        try {
            account.insert(context, new ArrayList<>() {{
                add(0, "1");
                add(1, "'KevinAxel'");
                add(2, "5000");
            }});
            account.insert(context, new ArrayList<>() {{
                add(0, "2");
                add(1, "'KAAAsS'");
                add(2, "8000");
            }});
            account.insert(context, new ArrayList<>() {{
                add(0, "3");
                add(1, "'kkk'");
                add(2, "8000");
            }});

        } catch (TableConflictException | TableExistenceException | ArgumentException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        // 测试插入结果
        try {
            var data = account.readAll(context);
            assertEquals(3, data.size());

            assertEquals(3, data.get(0).size());
            assertEquals(1, (int) data.get(0).get(0));
            assertEquals("KevinAxel", (String) data.get(0).get(1));
            assertEquals(5000f, data.get(0).get(2));

            assertEquals(3, data.get(1).size());
            assertEquals(2, (int) data.get(1).get(0));
            assertEquals("KAAAsS", (String) data.get(1).get(1));
            assertEquals(8000f, data.get(1).get(2));

            assertEquals(3, data.get(2).size());
            assertEquals(3, (int) data.get(2).get(0));
            assertEquals("kkk", (String) data.get(2).get(1));
            assertEquals(8000f, data.get(2).get(2));


        } catch (TableExistenceException | TableConflictException | ArgumentException | RecordNotFoundException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        // 创建payment表
        var paymentFields = new ArrayList<BaseField>();
        var paymentId = new IntField("ID", false, null);
        paymentFields.add(paymentId);
        paymentFields.add(new VarcharField("type", 1, false, null));
        Table payment = null;
        try {
            manager.createTable(context, "testSelect$payment", paymentFields, PATH + "testSelect.payment.db");
            paymentId.createIndex();
            payment = manager.getTable("testSelect$payment");
        } catch (TableExistenceException | IndexAlreadyExistException | RecordNotFoundException | ArgumentException | TableConflictException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        assertNotNull(payment);
        try {
            payment.insert(context, new ArrayList<>() {{
                add(0, "1");
                add(1, "'N'");
            }});
            payment.insert(context, new ArrayList<>() {{
                add(0, "2");
                add(1, "'T'");
            }});
            payment.insert(context, new ArrayList<>() {{
                add(0, "3");
                add(1, "'T'");
            }});

        } catch (TableConflictException | TableExistenceException | ArgumentException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        // 测试插入结果
        try {
            var data = payment.readAll(context);
            assertEquals(3, data.size());

            assertEquals(2, data.get(0).size());
            assertEquals(1, (int) data.get(0).get(0));
            assertEquals("N", (String) data.get(0).get(1));

            assertEquals(2, data.get(1).size());
            assertEquals(2, (int) data.get(1).get(0));
            assertEquals("T", (String) data.get(1).get(1));

            assertEquals(2, data.get(2).size());
            assertEquals(3, (int) data.get(2).get(0));
            assertEquals("T", (String) data.get(2).get(1));


        } catch (TableExistenceException | TableConflictException | ArgumentException | RecordNotFoundException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        // 执行语句
        var exe = new SelectExecutor((SelectStatement) stmt, manager, context);
        try {
            exe.execute();
        } catch (TableConflictException | ArgumentException | TableExistenceException | IndexAlreadyExistException | RecordNotFoundException e) {
            log.error("Exception expected: ", e);
            fail();
        }
        var cols = exe.getResultTable();
        var data = exe.getResultData();

        assertEquals(3, cols.size());
        assertEquals("testSelect$account", cols.get(0).getTableName());
        assertEquals("name", cols.get(0).getFieldName());
        assertEquals("testSelect$account", cols.get(1).getTableName());
        assertEquals("ID", cols.get(1).getFieldName());
        assertEquals("testSelect$account", cols.get(2).getTableName());
        assertEquals("balance", cols.get(2).getFieldName());

        assertEquals(2, data.size());

        assertEquals(3, data.get(0).size());
        assertEquals(3, data.get(0).get(0));
        assertEquals("kkk", data.get(0).get(1));
        assertEquals(8000f, data.get(0).get(2));

        assertEquals(3, data.get(0).size());
        assertEquals(2, data.get(1).get(0));
        assertEquals("KAAAsS", data.get(1).get(1));
        assertEquals(8000f, data.get(1).get(2));


        new File("metadata.db").deleteOnExit();

    }
}
