package net.kaaass.rumbase.table;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.table.exception.TableConflictException;
import net.kaaass.rumbase.table.field.BaseField;
import net.kaaass.rumbase.table.field.FloatField;
import net.kaaass.rumbase.table.field.IntField;
import net.kaaass.rumbase.table.field.VarcharField;

import java.io.ByteArrayInputStream;

/**
 * 字段结构测试
 *
 * @author @KveinAxel
 * @see BaseField
 */
@Slf4j
public class BaseFieldTest extends TestCase {

    public void testCheckStr() {

        // test int
        var intField = new IntField("testCheckStrInt");
        assertTrue(intField.checkStr("1"));
        assertFalse(intField.checkStr("1.2"));
        assertFalse(intField.checkStr("1aa"));

        // test float
        var floatField = new FloatField("testCheckStrFloat");
        assertTrue(floatField.checkStr("1"));
        assertTrue(floatField.checkStr("1.2"));
        assertFalse(floatField.checkStr("1aa"));

        // test varchar
        var varcharField = new VarcharField("testCheckStrVarchar", 20);
        assertTrue(varcharField.checkStr("aaaa"));
        assertFalse(varcharField.checkStr("aaaa aaaa aaaa aaaa aaaa"));

    }

    public void testDeserialize() {

        // 测试数据
        var bytes = new byte[]{
                0, 0, 0, 33, // int 33
                63, -103, -103, -102, // float 1.2
                12,
                116, 101, 115, 116,
                32, 118, 97, 114,
                99, 104, 97, 114 // varchar test varchar
        };
        var inputStream = new ByteArrayInputStream(bytes);

        var intField = new IntField("testDeserializeInt");
        try {
            var intRes = (int) intField.deserialize(inputStream);
            assertEquals(33, intRes);
        } catch (TableConflictException e) {
            log.error("Exception expected: ", e);
            fail("proper format should not fail to parse");
        }

        var floatField = new FloatField("testDeserializeFloat");
        try {
            var floatRes = (float) floatField.deserialize(inputStream);
            assertEquals(1.2f, floatRes);
        } catch (TableConflictException e) {
            log.error("Exception expected: ", e);
            fail("proper format should not fail to parse");
        }

        var varcharField = new VarcharField("testDeserializeVarchar", 20);
        try {
            var varcharRes = (String) varcharField.deserialize(inputStream);
            assertEquals("test varchar", varcharRes);
        } catch (TableConflictException e) {
            fail("proper format should not fail to parse");
        }

        assertEquals(0, inputStream.available());

    }

    public void testCheckInputStream() {
        // 测试数据
        var bytes = new byte[]{
                0, 0, 0, 33, // int 33
                63, -103, -103, -102, // float 1.2
                12,
                116, 101, 115, 116,
                32, 118, 97, 114,
                99, 104, 97, 114 // varchar test varchar
        };
        var inputStream = new ByteArrayInputStream(bytes);

        var intField = new IntField("testCheckInputStreamInt");
        assertTrue(intField.checkInputStream(inputStream));

        var floatField = new FloatField("testCheckInputStreamFloat");
        assertTrue(floatField.checkInputStream(inputStream));

        var varcharField = new VarcharField("testCheckInputStreamVarchar", 20);
        assertTrue(varcharField.checkInputStream(inputStream));

        assertEquals(0, inputStream.available());
    }

    public void testInsertIndex() {
        // todo
    }

    public void testQueryIndex() {
        // todo
    }
}
