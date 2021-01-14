package net.kaaass.rumbase.table;

import com.igormaznitsa.jbbp.io.JBBPBitOutputStream;
import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.index.exception.IndexAlreadyExistException;
import net.kaaass.rumbase.record.RecordManager;
import net.kaaass.rumbase.table.exception.TableConflictException;
import net.kaaass.rumbase.table.exception.TableExistenceException;
import net.kaaass.rumbase.table.field.*;
import net.kaaass.rumbase.transaction.TransactionContext;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;

import static org.junit.Assert.assertArrayEquals;

/**
 * 表结构测试
 *
 * @author @KveinAxel
 * @see Table
 */
@Slf4j
public class TableTest extends TestCase {

    public void testLoad() {
        var bytes = new byte[]{
                13,
                116, 101, 115, 116,
                76, 111, 97, 100,
                84, 97, 98, 108,
                101,

                6,
                78, 79, 82, 77,
                65, 76,

                -1, -1, -1, -1,
                -1, -1, -1, -1,

                0, 0, 0, 3,

                11,
                116, 101, 115, 116,
                76, 111, 97, 100,
                73, 110, 116,

                3,
                73, 78, 84,

                13,
                116, 101, 115, 116,
                76, 111, 97, 100,
                70, 108, 111, 97,
                116,

                5,
                70, 76, 79, 65,
                84,

                15,
                116, 101, 115, 116,
                76, 111, 97, 100,
                86, 97, 114, 99,
                104, 97, 114,

                7,
                86, 65, 82, 67,
                72, 65, 82,

                0, 0, 0, 12
        };

        var storage = RecordManager.fromFile("testLoadTable");
        storage.setMetadata(TransactionContext.empty(), bytes);

        var table = Table.load("testLoadTable");

        assertNotNull(table);
        assertEquals("testLoadTable", table.getTableName());
        assertEquals(-1L, table.getNext());
        assertEquals(TableStatus.NORMAL, table.getStatus());

        var fields = table.getFields();
        assertEquals("testLoadInt", fields.get(0).getName());
        assertEquals(FieldType.INT, fields.get(0).getType());
        assertEquals("testLoadFloat", fields.get(1).getName());
        assertEquals(FieldType.FLOAT, fields.get(1).getType());
        assertEquals("testLoadVarchar", fields.get(2).getName());
        assertEquals(FieldType.VARCHAR, fields.get(2).getType());
        assertEquals(12, ((VarcharField)fields.get(2)).getLimit());
    }

    public void testPersist() {
        var fieldList = new ArrayList<BaseField>();
        var table = new Table("testPersistTable", fieldList);
        var context = TransactionContext.empty();

        fieldList.add(new IntField("testPersistInt", table));
        fieldList.add(new FloatField("testPersistFloat", table));
        fieldList.add(new VarcharField("testPersistVarchar", 12, table));

        var expected = new byte[]{
                // testPersistTable
                16,
                116, 101, 115, 116,
                80, 101, 114, 115,
                105, 115, 116, 84,
                97, 98, 108, 101,

                // NORMAL
                6,
                78, 79, 82, 77,
                65, 76,

                // -1
                -1, -1, -1, -1,
                -1, -1, -1, -1,

                // 3
                0, 0, 0, 3,

                // testPersistInt
                14,
                116, 101, 115, 116,
                80, 101, 114, 115,
                105, 115, 116, 73,
                110, 116,

                // INT
                3,
                73, 78, 84,

                // testPersistFloat
                16,
                116, 101, 115, 116,
                80, 101, 114, 115,
                105, 115, 116, 70,
                108, 111, 97, 116,

                // FLOAT
                5,
                70, 76, 79, 65,
                84,

                // testPersistVarchar
                18,
                116, 101, 115, 116,
                80, 101, 114, 115,
                105, 115, 116, 86,
                97, 114, 99, 104,
                97, 114,

                // VARCHAR
                7,
                86, 65, 82, 67,
                72, 65, 82,

                // 12
                0, 0, 0, 12
        };

        assertArrayEquals(new byte[0], table.getRecordStorage().getMetadata(context));
        table.persist(TransactionContext.empty());
        assertArrayEquals(expected, table.getRecordStorage().getMetadata(context));

    }

    public void testCURD() { // todo 未完成
        var storage = RecordManager.fromFile("testDelete");

        var fieldList = new ArrayList<BaseField>();
        var table = new Table("testPersistTable", fieldList);
        var context = TransactionContext.empty();

        var intField = new IntField("testPersistInt", table);
        var floatField = new FloatField("testPersistFloat", table);
        var varcharField = new VarcharField("testPersistVarchar", 12, table);
        fieldList.add(intField);
        fieldList.add(floatField);
        fieldList.add(varcharField);

        try {
            intField.createIndex();
            floatField.createIndex();
        } catch (IndexAlreadyExistException e) {
            log.error("Exception expected: ", e);
            fail();          }

        var data = new ArrayList<String>();
        data.add("33");
        data.add("1.2");
        data.add("test varchar");
        try {
            table.insert(context, data);
            // todo
        } catch (TableConflictException | TableExistenceException e) {
            log.error("Exception expected: ", e);
            fail();
        }
    }

    public void testSearch() {
        // todo
    }

    public void testCheckStringEntry() {
        var passEntry = new ArrayList<String>();
        passEntry.add("33");
        passEntry.add("1.2");
        passEntry.add("test varchar");

        var failEntry = new ArrayList<String>();
        failEntry.add("33");
        failEntry.add("test varchar");
        failEntry.add("1.2");

        var fieldList = new ArrayList<BaseField>();
        var table = new Table("testCheckStringEntryTable", fieldList);
        var intField = new IntField("testCheckStringEntryInt", table);
        var floatField = new FloatField("testCheckStringEntryFloat", table);
        var varcharField = new VarcharField("testCheckStringEntryVarchar", 20, table);
        fieldList.add(intField);
        fieldList.add(floatField);
        fieldList.add(varcharField);

        assertTrue(table.checkStringEntry(passEntry));
        assertFalse(table.checkStringEntry(failEntry));

    }

    public void testStringEntryToBytes() {
        var passEntry = new ArrayList<String>();
        passEntry.add("33");
        passEntry.add("1.2");
        passEntry.add("test varchar");

        var failEntry = new ArrayList<String>();
        failEntry.add("33");
        failEntry.add("test varchar");
        failEntry.add("1.2");

        var fieldList = new ArrayList<BaseField>();
        var table = new Table("testStringEntryToBytesTable", fieldList);
        var intField = new IntField("testStringEntryToBytesInt", table);
        var floatField = new FloatField("testStringEntryToBytesFloat", table);
        var varcharField = new VarcharField("testStringEntryToBytesVarchar", 20, table);
        fieldList.add(intField);
        fieldList.add(floatField);
        fieldList.add(varcharField);

        try {
            var bytes = table.stringEntryToBytes(passEntry);
            var expected = new byte[]{
                    0, 0, 0, 33, // int 33
                    63, -103, -103, -102, // float 1.2
                    12,
                    116, 101, 115, 116,
                    32, 118, 97, 114,
                    99, 104, 97, 114 // varchar test varchar
            };
            assertArrayEquals(expected, bytes);
        } catch (TableConflictException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        try {
            table.stringEntryToBytes(failEntry);
            fail("");
        } catch (TableConflictException e) {
            log.error("Exception expected: ", e);
        }
    }

    public void testParseEntry() {
        var passEntry = new byte[]{
                0, 0, 0, 33, // int 33
                63, -103, -103, -102, // float 1.2
                12,
                116, 101, 115, 116,
                32, 118, 97, 114,
                99, 104, 97, 114 // varchar test varchar
        };

        var failEntry = new byte[]{
                0, 0, 0, 33, // int 33
                12,
                116, 101, 115, 116,
                32, 118, 97, 114,
                99, 104, 97, 114, // varchar test varchar
                63, -103, -103, -102, // float 1.2
        };

        var fieldList = new ArrayList<BaseField>();
        var table = new Table("testParseEntryTable", fieldList);
        var intField = new IntField("testParseEntryInt", table);
        var floatField = new FloatField("testParseEntryFloat", table);
        var varcharField = new VarcharField("testParseEntryVarchar", 20, table);
        fieldList.add(intField);
        fieldList.add(floatField);
        fieldList.add(varcharField);

        try {
            var list = table.parseEntry(passEntry);
            assertEquals(33, (int) list.get(0));
            assertEquals(1.2f, list.get(1));
            assertEquals("test varchar", (String) list.get(2));
        } catch (TableConflictException | IOException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        try {
            table.parseEntry(failEntry);
            fail();
        } catch (TableConflictException | IOException e) {
            log.error("Exception expected: ", e);
        }

    }


}
