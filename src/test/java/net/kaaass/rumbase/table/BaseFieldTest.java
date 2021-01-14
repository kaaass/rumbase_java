package net.kaaass.rumbase.table;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.index.exception.IndexAlreadyExistException;
import net.kaaass.rumbase.table.exception.TableConflictException;
import net.kaaass.rumbase.table.exception.TableExistenceException;
import net.kaaass.rumbase.table.field.*;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;

import static org.junit.Assert.assertArrayEquals;

/**
 * 字段结构测试
 *
 * @author @KveinAxel
 * @see BaseField
 */
@Slf4j
public class BaseFieldTest extends TestCase {

    public void testCheckStr() {

        var dummy = new Table("testCheckStrTable", new ArrayList<>());

        // test int
        var intField = new IntField("testCheckStrInt", dummy);
        assertTrue(intField.checkStr("1"));
        assertFalse(intField.checkStr("1.2"));
        assertFalse(intField.checkStr("1aa"));

        // test float
        var floatField = new FloatField("testCheckStrFloat", dummy);
        assertTrue(floatField.checkStr("1"));
        assertTrue(floatField.checkStr("1.2"));
        assertFalse(floatField.checkStr("1aa"));

        // test varchar
        var varcharField = new VarcharField("testCheckStrVarchar", 20, dummy);
        assertTrue(varcharField.checkStr("aaaa"));
        assertFalse(varcharField.checkStr("aaaa aaaa aaaa aaaa aaaa"));

    }

    public void testDeserialize() {

        var dummy = new Table("testDeserializeTable", new ArrayList<>());

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

        var intField = new IntField("testDeserializeInt", dummy);
        try {
            var intRes = (int) intField.deserialize(inputStream);
            assertEquals(33, intRes);
        } catch (TableConflictException e) {
            log.error("Exception expected: ", e);
            fail("proper format should not fail to parse");
        }

        var floatField = new FloatField("testDeserializeFloat", dummy);
        try {
            var floatRes = (float) floatField.deserialize(inputStream);
            assertEquals(1.2f, floatRes);
        } catch (TableConflictException e) {
            log.error("Exception expected: ", e);
            fail("proper format should not fail to parse");
        }

        var varcharField = new VarcharField("testDeserializeVarchar", 20, dummy);
        try {
            var varcharRes = (String) varcharField.deserialize(inputStream);
            assertEquals("test varchar", varcharRes);
        } catch (TableConflictException e) {
            fail("proper format should not fail to parse");
        }

        assertEquals(0, inputStream.available());

    }

    public void testCheckInputStream() {

        var dummy = new Table("testCheckInputStreamTable", new ArrayList<>());

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

        var intField = new IntField("testCheckInputStreamInt", dummy);
        assertTrue(intField.checkInputStream(inputStream));

        var floatField = new FloatField("testCheckInputStreamFloat", dummy);
        assertTrue(floatField.checkInputStream(inputStream));

        var varcharField = new VarcharField("testCheckInputStreamVarchar", 20, dummy);
        assertTrue(varcharField.checkInputStream(inputStream));

        assertEquals(0, inputStream.available());
    }

    public void testSerialize() {
        var dummy = new Table("testSerialize", new ArrayList<>());


        var intField = new IntField("testSerializeInt", dummy);
        var intBos1 = new ByteArrayOutputStream();
        var intBos2 = new ByteArrayOutputStream();

        try {
            intField.serialize(intBos1, "33");
            var expected = new byte[]{
                    0, 0, 0, 33, // int 33
            };
            assertArrayEquals(expected, intBos1.toByteArray());
        } catch (TableConflictException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        try {
            intField.serialize(intBos2, "xx");
            fail();
        } catch (TableConflictException e) {
            log.error("Exception expected: ", e);
        }

        var floatField = new FloatField("testSerializeFloat", dummy);
        var floatBos1 = new ByteArrayOutputStream();
        var floatBos2 = new ByteArrayOutputStream();

        try {
            floatField.serialize(floatBos1, "1.2");
            var expected = new byte[]{
                    63, -103, -103, -102, // float 1.2
            };
            assertArrayEquals(expected, floatBos1.toByteArray());
        } catch (TableConflictException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        try {
            floatField.serialize(floatBos2, "xx");
            fail();
        } catch (TableConflictException e) {
            log.error("Exception expected: ", e);
        }

        var varcharField = new VarcharField("testSerializeVarchar", 20, dummy);
        var varcharBos1 = new ByteArrayOutputStream();
        var varcharBos2 = new ByteArrayOutputStream();

        try {
            varcharField.serialize(varcharBos1, "test varchar");
            var expected = new byte[]{
                    12,
                    116, 101, 115, 116,
                    32, 118, 97, 114,
                    99, 104, 97, 114 // varchar test varchar
            };
            assertArrayEquals(expected, varcharBos1.toByteArray());
        } catch (TableConflictException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        try {
            varcharField.serialize(varcharBos2, "test varchar too looooooooooooong");
            fail();
        } catch (TableConflictException e) {
            log.error("Exception expected: ", e);
        }

    }

    public void testDoubleCreateIndex() {
        var dummy = new Table("testCreateIndexTable", new ArrayList<>());

        BaseField field = new IntField("testCreateIndexField", dummy);
        try {
            field.createIndex();
        } catch (IndexAlreadyExistException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        try {
            field.createIndex();
            fail();
        } catch (IndexAlreadyExistException e) {
            log.error("Exception expected: ", e);
        }
    }

    public void testInsertIndex() {
        var dummy = new Table("testInsertIndexTable", new ArrayList<>());

        var intField = new IntField("testInsertIndexInt", dummy);

        try {
            intField.insertIndex("1", 1);
            fail();
        } catch (TableExistenceException | TableConflictException e) {
            log.error("Exception expected: ", e);
        }

        try {
            intField.createIndex();
        } catch (IndexAlreadyExistException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        try {
            intField.insertIndex("1", 1);
            intField.insertIndex("1", 1);
        } catch (TableExistenceException | TableConflictException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        try {
            intField.insertIndex("1xx", 1);
            fail();
        } catch (TableExistenceException | TableConflictException e) {
            log.error("Exception expected: ", e);
        }

        var floatField = new FloatField("testInsertIndexFloat", dummy);

        try {
            floatField.insertIndex("1.2", 1);
            fail();
        } catch (TableExistenceException | TableConflictException e) {
            log.error("Exception expected: ", e);
        }

        try {
            floatField.createIndex();
        } catch (IndexAlreadyExistException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        try {
            floatField.insertIndex("1.2", 1);
            floatField.insertIndex("1.2", 1);
        } catch (TableExistenceException | TableConflictException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        try {
            floatField.insertIndex("1xx", 1);
            fail();
        } catch (TableExistenceException | TableConflictException e) {
            log.error("Exception expected: ", e);
        }


        var varcharField = new VarcharField("testInsertIndexVarchar", 20, dummy);

        try {
            varcharField.insertIndex("xxx", 1);
            fail();
        } catch (TableExistenceException e) {
            log.error("Exception expected: ", e);
        }

        try {
            varcharField.createIndex();
        } catch (IndexAlreadyExistException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        try {
            varcharField.insertIndex("xxx", 1);
            varcharField.insertIndex("xxx", 1);
        } catch (TableExistenceException e) {
            log.error("Exception expected: ", e);
            fail();
        }

    }

    public void testQueryIndex() {
        var dummy = new Table("testQueryIndexTable", new ArrayList<>());

        var intField = new IntField("testQueryIndexInt", dummy);

        try {
            intField.createIndex();
        } catch (IndexAlreadyExistException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        try {
            intField.insertIndex("1", 1);
            intField.insertIndex("1", 1);
        } catch (TableExistenceException | TableConflictException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        try {
            var uuid = intField.queryIndex("1");
            assertEquals(1, uuid.get(0).longValue());
            assertEquals(1, uuid.get(1).longValue());
        } catch (TableExistenceException | TableConflictException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        try {
            var uuid = intField.queryIndex("2");
            assertTrue(uuid.isEmpty());
        } catch (TableExistenceException | TableConflictException e) {
            log.error("Exception expected: ", e);
            fail();        }

        var floatField = new FloatField("testQueryIndexFloat", dummy);

        try {
            floatField.createIndex();
        } catch (IndexAlreadyExistException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        try {
            floatField.insertIndex("1.2", 1);
            floatField.insertIndex("1.2", 1);
        } catch (TableExistenceException | TableConflictException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        try {
            var uuid = floatField.queryIndex("1.2");
            assertEquals(1, uuid.get(0).longValue());
            assertEquals(1, uuid.get(1).longValue());
        } catch (TableExistenceException | TableConflictException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        try {
            var uuid = floatField.queryIndex("2.2");
            assertTrue(uuid.isEmpty());
        } catch (TableExistenceException | TableConflictException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        var varcharField = new VarcharField("testQueryIndexVarchar", 20, dummy);

        try {
            varcharField.createIndex();
        } catch (IndexAlreadyExistException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        try {
            varcharField.insertIndex("xxx", 1);
            varcharField.insertIndex("xxx", 1);
        } catch (TableExistenceException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        try {
            var uuid = varcharField.queryIndex("xxx");
            assertEquals(1, uuid.get(0).longValue());
            assertEquals(1, uuid.get(1).longValue());
        } catch (TableExistenceException e) {
            log.error("Exception expected: ", e);
            fail();
        }

        try {
            var uuid = varcharField.queryIndex("x");
            assertTrue(uuid.isEmpty());
        } catch (TableExistenceException e) {
            log.error("Exception expected: ", e);
            fail();        }
    }

    public void testLoad() {
        var bytes = new byte[]{
                // testLoadInt
                11,
                116, 101, 115, 116,
                76, 111, 97, 100,
                73, 110, 116,

                // INT
                3,
                73, 78, 84,

                // testLoadFloat
                13,
                116, 101, 115, 116,
                76, 111, 97, 100,
                70, 108, 111, 97, 116,

                // FLOAT
                5,
                70, 76, 79, 65, 84,

                // testLoadVarchar
                15,
                116, 101, 115, 116,
                76, 111, 97, 100,
                86, 97, 114, 99,
                104, 97, 114,

                // VARCHAR
                7,
                86, 65, 82, 67,
                72, 65, 82,

                // 12
                0, 0, 0, 12

        };
        var stream = new ByteArrayInputStream(bytes);
        var dummy = new Table("testLoadTable", new ArrayList<>());

        var intField = BaseField.load(stream, dummy);
        assertNotNull(intField);
        assertEquals("testLoadInt", intField.getName());
        assertEquals(FieldType.INT, intField.getType());

        var floatField = BaseField.load(stream, dummy);
        assertNotNull(floatField);
        assertEquals("testLoadFloat", floatField.getName());
        assertEquals(FieldType.FLOAT, floatField.getType());

        var varcharField = BaseField.load(stream, dummy);
        assertNotNull(varcharField);
        assertEquals("testLoadVarchar", varcharField.getName());
        assertEquals(FieldType.VARCHAR, varcharField.getType());
        assertEquals(12, ((VarcharField)varcharField).getLimit());

    }

    public void testPersist() {
        var dummy = new Table("testLoadTable", new ArrayList<>());
        var out = new ByteArrayOutputStream();

        var intField = new IntField("testPersistInt", dummy);
        var floatField = new FloatField("testPersistFloat", dummy);
        var varcharField = new VarcharField("testPersistVarchar", 12, dummy);

        intField.persist(out);
        floatField.persist(out);
        varcharField.persist(out);

        var expected = new byte[]{
                // testPersistInt
                14,
                116, 101, 115, 116,
                80, 101, 114, 115,
                105, 115, 116, 73, 110, 116,

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
                70, 76, 79, 65, 84,

                // testPersistVarchar
                18,
                116, 101, 115, 116,
                80, 101, 114, 115,
                105, 115, 116, 86,
                97, 114, 99, 104, 97, 114,

                // VARCHAR
                7,
                86, 65, 82, 67,
                72, 65, 82,

                // 12
                0, 0, 0, 12
        };

        var res = out.toByteArray();
        assertArrayEquals(expected, out.toByteArray());
    }
}
