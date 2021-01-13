package net.kaaass.rumbase.table;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.table.exception.TableConflictException;
import net.kaaass.rumbase.table.field.BaseField;
import net.kaaass.rumbase.table.field.FloatField;
import net.kaaass.rumbase.table.field.IntField;
import net.kaaass.rumbase.table.field.VarcharField;

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

    public void testParseSelf() {
        // todo
    }

    public void testPersistSelf() {
        // todo
    }

    public void testDelete() {
        // todo
    }

    public void testUpdate() {
        // todo
    }

    public void testRead() {
        // todo
    }

    public void testInsert() {
        // todo
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

        var intField = new IntField("testCheckStringEntryInt");
        var floatField = new FloatField("testCheckStringEntryFloat");
        var varcharField = new VarcharField("testCheckStringEntryVarchar", 20);
        var fieldList = new ArrayList<BaseField>();
        fieldList.add(intField);
        fieldList.add(floatField);
        fieldList.add(varcharField);
        var table = new Table("testCheckStringEntryTable", fieldList);

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

        var intField = new IntField("testStringEntryToBytesInt");
        var floatField = new FloatField("testStringEntryToBytesFloat");
        var varcharField = new VarcharField("testStringEntryToBytesVarchar", 20);
        var fieldList = new ArrayList<BaseField>();
        fieldList.add(intField);
        fieldList.add(floatField);
        fieldList.add(varcharField);
        var table = new Table("testStringEntryToBytesTable", fieldList);

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

        var intField = new IntField("testParseEntryInt");
        var floatField = new FloatField("testParseEntryFloat");
        var varcharField = new VarcharField("testParseEntryVarchar", 20);
        var fieldList = new ArrayList<BaseField>();
        fieldList.add(intField);
        fieldList.add(floatField);
        fieldList.add(varcharField);
        var table = new Table("testParseEntryTable", fieldList);

        try {
            var list = table.parseEntry(passEntry);
            assertEquals(33, (int) list.get(0));
            assertEquals(1.2f, (float) list.get(1));
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
