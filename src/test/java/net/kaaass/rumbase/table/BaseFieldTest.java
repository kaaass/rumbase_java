package net.kaaass.rumbase.table;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.FileUtil;
import net.kaaass.rumbase.index.exception.IndexAlreadyExistException;
import net.kaaass.rumbase.index.exception.IndexNotFoundException;
import net.kaaass.rumbase.table.exception.TableConflictException;
import net.kaaass.rumbase.table.exception.TableExistenceException;
import net.kaaass.rumbase.table.field.*;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

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
public class BaseFieldTest {

    @BeforeClass
    @AfterClass
    public static void clearDataFolder() {
        log.info("清除数据文件夹...");
        FileUtil.removeDir(FileUtil.DATA_PATH);
    }

    @Test
    public void testCheckStr() {

        // test int
        var intField = new IntField("testCheckStrInt", false, null);
        Assert.assertTrue(intField.checkStr("1"));
        Assert.assertFalse(intField.checkStr("1.2"));
        Assert.assertFalse(intField.checkStr("1aa"));

        // test float
        var floatField = new FloatField("testCheckStrFloat", false, null);
        Assert.assertTrue(floatField.checkStr("1"));
        Assert.assertTrue(floatField.checkStr("1.2"));
        Assert.assertFalse(floatField.checkStr("1aa"));

        // test varchar
        var varcharField = new VarcharField("testCheckStrVarchar", 20, false, null);
        Assert.assertTrue(varcharField.checkStr("'aaaa'"));
        Assert.assertFalse(varcharField.checkStr("'aaaa aaaa aaaa aaaa aaaa'"));

    }

    @Test
    public void testDeserialize() {

        // 测试数据
        var bytes = new byte[]{
                0,
                0, 0, 0, 33, // int 33
                0,
                63, -103, -103, -102, // float 1.2
                0,
                12,
                116, 101, 115, 116,
                32, 118, 97, 114,
                99, 104, 97, 114 // varchar test varchar
        };
        var inputStream = new ByteArrayInputStream(bytes);

        var intField = new IntField("testDeserializeInt", false, null);
        try {
            var intRes = (int) intField.deserialize(inputStream);
            Assert.assertEquals(33, intRes);
        } catch (TableConflictException e) {
            log.error("Exception expected: ", e);
            Assert.fail("proper format should not fail to parse");
        }

        var floatField = new FloatField("testDeserializeFloat", false, null);
        try {
            var floatRes = (float) floatField.deserialize(inputStream);
            Assert.assertTrue(1.2f - floatRes < 0.0001);
        } catch (TableConflictException e) {
            log.error("Exception expected: ", e);
            Assert.fail("proper format should not fail to parse");
        }

        var varcharField = new VarcharField("testDeserializeVarchar", 20, false, null);
        try {
            var varcharRes = (String) varcharField.deserialize(inputStream);
            Assert.assertEquals("test varchar", varcharRes);
        } catch (TableConflictException e) {
            Assert.fail("proper format should not fail to parse");
        }

        Assert.assertEquals(0, inputStream.available());

    }

    @Test
    public void testCheckInputStream() {

        // 测试数据
        var bytes = new byte[]{
                0,
                0, 0, 0, 33, // int 33
                0,
                63, -103, -103, -102, // float 1.2
                0,
                12,
                116, 101, 115, 116,
                32, 118, 97, 114,
                99, 104, 97, 114 // varchar test varchar
        };
        var inputStream = new ByteArrayInputStream(bytes);

        var intField = new IntField("testCheckInputStreamInt", false, null);
        Assert.assertTrue(intField.checkInputStream(inputStream));

        var floatField = new FloatField("testCheckInputStreamFloat", false, null);
        Assert.assertTrue(floatField.checkInputStream(inputStream));

        var varcharField = new VarcharField("testCheckInputStreamVarchar", 20, false, null);
        Assert.assertTrue(varcharField.checkInputStream(inputStream));

        Assert.assertEquals(0, inputStream.available());
    }

    @Test
    public void testSerialize() {


        var intField = new IntField("testSerializeInt", false, null);
        var intBos1 = new ByteArrayOutputStream();
        var intBos2 = new ByteArrayOutputStream();

        try {
            intField.serialize(intBos1, "33");
            var expected = new byte[]{
                    0,
                    0, 0, 0, 33, // int 33
            };
            assertArrayEquals(expected, intBos1.toByteArray());
        } catch (TableConflictException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

        try {
            intField.serialize(intBos2, "'xx'");
            Assert.fail();
        } catch (TableConflictException e) {
            log.error("Exception expected: ", e);
        }

        var floatField = new FloatField("testSerializeFloat", false, null);
        var floatBos1 = new ByteArrayOutputStream();
        var floatBos2 = new ByteArrayOutputStream();

        try {
            floatField.serialize(floatBos1, "1.2");
            var expected = new byte[]{
                    0,
                    63, -103, -103, -102, // float 1.2
            };
            assertArrayEquals(expected, floatBos1.toByteArray());
        } catch (TableConflictException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

        try {
            floatField.serialize(floatBos2, "'xx'");
            Assert.fail();
        } catch (TableConflictException e) {
            log.error("Exception expected: ", e);
        }

        var varcharField = new VarcharField("testSerializeVarchar", 20, false, null);
        var varcharBos1 = new ByteArrayOutputStream();
        var varcharBos2 = new ByteArrayOutputStream();

        try {
            varcharField.serialize(varcharBos1, "'test varchar'");
            var expected = new byte[]{
                    0,
                    12,
                    116, 101, 115, 116,
                    32, 118, 97, 114,
                    99, 104, 97, 114 // varchar test varchar
            };
            assertArrayEquals(expected, varcharBos1.toByteArray());
        } catch (TableConflictException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

        try {
            varcharField.serialize(varcharBos2, "'test varchar too looooooooooooong'");
            Assert.fail();
        } catch (TableConflictException e) {
            log.error("Exception expected: ", e);
        }

    }

    @Test
    public void testDoubleCreateIndex() {
        var dummy = new Table("testCreateIndexTable", new ArrayList<>());

        BaseField field = new IntField("testCreateIndexField", false, dummy);
        try {
            field.createIndex();
        } catch (IndexAlreadyExistException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

        try {
            field.createIndex();
            Assert.fail();
        } catch (IndexAlreadyExistException e) {
            log.error("Exception expected: ", e);
        }
    }

    @Test
    public void testInsertIndex() {
        var dummy = new Table("testInsertIndexTable", new ArrayList<>());

        var intField = new IntField("testInsertIndexInt", false, dummy);

        try {
            intField.insertIndex("1", 1);
            Assert.fail();
        } catch (TableExistenceException | TableConflictException e) {
            log.error("Exception expected: ", e);
        }

        try {
            intField.createIndex();
        } catch (IndexAlreadyExistException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

        try {
            intField.insertIndex("1", 1);
            intField.insertIndex("1", 1);
        } catch (TableExistenceException | TableConflictException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

        try {
            intField.insertIndex("1xx", 1);
            Assert.fail();
        } catch (TableExistenceException | TableConflictException e) {
            log.error("Exception expected: ", e);
        }

        var floatField = new FloatField("testInsertIndexFloat", false, dummy);

        try {
            floatField.insertIndex("1.2", 1);
            Assert.fail();
        } catch (TableExistenceException | TableConflictException e) {
            log.error("Exception expected: ", e);
        }

        try {
            floatField.createIndex();
        } catch (IndexAlreadyExistException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

        try {
            floatField.insertIndex("1.2", 1);
            floatField.insertIndex("1.2", 1);
        } catch (TableExistenceException | TableConflictException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

        try {
            floatField.insertIndex("1xx", 1);
            Assert.fail();
        } catch (TableExistenceException | TableConflictException e) {
            log.error("Exception expected: ", e);
        }


        var varcharField = new VarcharField("testInsertIndexVarchar", 20, false, dummy);

        try {
            varcharField.insertIndex("xxx", 1);
            Assert.fail();
        } catch (TableExistenceException e) {
            log.error("Exception expected: ", e);
        }

        try {
            varcharField.createIndex();
        } catch (IndexAlreadyExistException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

        try {
            varcharField.insertIndex("xxx", 1);
            varcharField.insertIndex("xxx", 1);
        } catch (TableExistenceException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

    }

    @Test
    public void testQueryIndex() {

        var dummy = new Table("testQueryIndexTable", new ArrayList<>());

        var intField = new IntField("testQueryIndexInt", false, dummy);

        try {
            intField.createIndex();
        } catch (IndexAlreadyExistException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

        try {
            intField.insertIndex("1", 1);
            intField.insertIndex("1", 1);
        } catch (TableExistenceException | TableConflictException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

        try {
            var uuid = intField.queryIndex("1");
            Assert.assertEquals(1, uuid.get(0).longValue());
            Assert.assertEquals(1, uuid.get(1).longValue());
        } catch (TableExistenceException | TableConflictException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

        try {
            var uuid = intField.queryIndex("2");
            Assert.assertTrue(uuid.isEmpty());
        } catch (TableExistenceException | TableConflictException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

        var floatField = new FloatField("testQueryIndexFloat", false, dummy);

        try {
            floatField.createIndex();
        } catch (IndexAlreadyExistException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

        try {
            floatField.insertIndex("1.2", 1);
            floatField.insertIndex("1.2", 1);
        } catch (TableExistenceException | TableConflictException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

        try {
            var uuid = floatField.queryIndex("1.2");
            Assert.assertEquals(1, uuid.get(0).longValue());
            Assert.assertEquals(1, uuid.get(1).longValue());
        } catch (TableExistenceException | TableConflictException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

        try {
            var uuid = floatField.queryIndex("2.2");
            Assert.assertTrue(uuid.isEmpty());
        } catch (TableExistenceException | TableConflictException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

        var varcharField = new VarcharField("testQueryIndexVarchar", 20, false, dummy);

        try {
            varcharField.createIndex();
        } catch (IndexAlreadyExistException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

        try {
            varcharField.insertIndex("xxx", 1);
            varcharField.insertIndex("xxx", 1);
        } catch (TableExistenceException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

        try {
            var uuid = varcharField.queryIndex("xxx");
            Assert.assertEquals(1, uuid.get(0).longValue());
            Assert.assertEquals(1, uuid.get(1).longValue());
        } catch (TableExistenceException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

        try {
            var uuid = varcharField.queryIndex("x");
            Assert.assertTrue(uuid.isEmpty());
        } catch (TableExistenceException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }
    }

    @Test
    public void testLoad() throws IndexNotFoundException {
        var bytes = new byte[]{
                // testLoadInt
                11,
                116, 101, 115, 116,
                76, 111, 97, 100,
                73, 110, 116,

                // INT
                3,
                73, 78, 84,

                // 00 not nullable and no index
                0,

                // testLoadFloat
                13,
                116, 101, 115, 116,
                76, 111, 97, 100,
                70, 108, 111, 97, 116,

                // FLOAT
                5,
                70, 76, 79, 65, 84,

                0,

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

                0,

                // 12
                0, 0, 0, 12

        };
        var stream = new ByteArrayInputStream(bytes);

        var intField = BaseField.load(stream, null);
        Assert.assertNotNull(intField);
        Assert.assertEquals("testLoadInt", intField.getName());
        Assert.assertEquals(FieldType.INT, intField.getType());

        var floatField = BaseField.load(stream, null);
        Assert.assertNotNull(floatField);
        Assert.assertEquals("testLoadFloat", floatField.getName());
        Assert.assertEquals(FieldType.FLOAT, floatField.getType());

        var varcharField = BaseField.load(stream, null);
        Assert.assertNotNull(varcharField);
        Assert.assertEquals("testLoadVarchar", varcharField.getName());
        Assert.assertEquals(FieldType.VARCHAR, varcharField.getType());
        Assert.assertEquals(12, ((VarcharField) varcharField).getLimit());

    }

    @Test
    public void testPersist() {
        var out = new ByteArrayOutputStream();

        var intField = new IntField("testPersistInt", false, null);
        var floatField = new FloatField("testPersistFloat", false, null);
        var varcharField = new VarcharField("testPersistVarchar", 12, false, null);

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

                0,

                // testPersistFloat
                16,
                116, 101, 115, 116,
                80, 101, 114, 115,
                105, 115, 116, 70,
                108, 111, 97, 116,

                // FLOAT
                5,
                70, 76, 79, 65, 84,

                0,

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

                0,

                // 12
                0, 0, 0, 12
        };

        assertArrayEquals(expected, out.toByteArray());
    }
}
