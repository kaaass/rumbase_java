package net.kaaass.rumbase.table;

import com.igormaznitsa.jbbp.io.JBBPBitOutputStream;
import com.igormaznitsa.jbbp.io.JBBPByteOrder;
import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.FileUtil;
import net.kaaass.rumbase.index.exception.IndexAlreadyExistException;
import net.kaaass.rumbase.index.exception.IndexNotFoundException;
import net.kaaass.rumbase.query.exception.ArgumentException;
import net.kaaass.rumbase.record.RecordManager;
import net.kaaass.rumbase.record.exception.RecordNotFoundException;
import net.kaaass.rumbase.table.exception.TableConflictException;
import net.kaaass.rumbase.table.exception.TableExistenceException;
import net.kaaass.rumbase.table.field.*;
import net.kaaass.rumbase.transaction.TransactionContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertArrayEquals;

/**
 * 表结构测试
 *
 * @author @KveinAxel
 * @see Table
 */
@Slf4j
public class TableTest {

    @BeforeClass
    public static void createDataFolder() {
        log.info("创建测试文件夹...");
        FileUtil.createDir(FileUtil.TEST_PATH);
        FileUtil.removeDir(FileUtil.DATA_PATH);
    }

    @AfterClass
    public static void clearDataFolder() {
        log.info("清除测试文件夹...");
        FileUtil.removeDir(FileUtil.TEST_PATH);
        FileUtil.removeDir(FileUtil.DATA_PATH);
    }

    @Test
    public void testLoad() {
        var prefix = "testLoad";

        var byteOS = new ByteArrayOutputStream();
        var out = new JBBPBitOutputStream(byteOS);
        try {
            out.writeString("testLoadTable", JBBPByteOrder.BIG_ENDIAN);
            out.writeString("NORMAL", JBBPByteOrder.BIG_ENDIAN);
            out.writeLong(-1, JBBPByteOrder.BIG_ENDIAN);
            out.writeInt(3, JBBPByteOrder.BIG_ENDIAN);
            out.writeString("testLoadInt", JBBPByteOrder.BIG_ENDIAN);
            out.writeString("INT", JBBPByteOrder.BIG_ENDIAN);
            out.writeBytes(new byte[]{0}, 1, JBBPByteOrder.BIG_ENDIAN);
            out.writeString("testLoadFloat", JBBPByteOrder.BIG_ENDIAN);
            out.writeString("FLOAT", JBBPByteOrder.BIG_ENDIAN);
            out.writeBytes(new byte[]{0}, 1, JBBPByteOrder.BIG_ENDIAN);
            out.writeString("testLoadVarchar", JBBPByteOrder.BIG_ENDIAN);
            out.writeString("VARCHAR", JBBPByteOrder.BIG_ENDIAN);
            out.writeBytes(new byte[]{0}, 1, JBBPByteOrder.BIG_ENDIAN);
            out.writeInt(12, JBBPByteOrder.BIG_ENDIAN);
        } catch (IOException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

        var storage = RecordManager.fromFile(FileUtil.TEST_PATH + prefix + "Table");
        storage.setMetadata(TransactionContext.empty(), byteOS.toByteArray());

        var table = Table.load(RecordManager.fromFile(FileUtil.TEST_PATH + prefix + "Table"));

        Assert.assertNotNull(table);
        Assert.assertEquals("testLoadTable", table.getTableName());
        Assert.assertEquals(-1L, table.getNext());
        Assert.assertEquals(TableStatus.NORMAL, table.getStatus());

        var fields = table.getFields();
        Assert.assertEquals("testLoadInt", fields.get(0).getName());
        Assert.assertEquals(FieldType.INT, fields.get(0).getType());
        Assert.assertEquals("testLoadFloat", fields.get(1).getName());
        Assert.assertEquals(FieldType.FLOAT, fields.get(1).getType());
        Assert.assertEquals("testLoadVarchar", fields.get(2).getName());
        Assert.assertEquals(FieldType.VARCHAR, fields.get(2).getType());
        Assert.assertEquals(12, ((VarcharField) fields.get(2)).getLimit());
    }

    @Test
    public void testPersist() {

        var fieldList = new ArrayList<BaseField>();
        var table = new Table("testPersistTable", fieldList);
        var context = TransactionContext.empty();

        fieldList.add(new IntField("testPersistInt", false, table));
        fieldList.add(new FloatField("testPersistFloat", false, table));
        fieldList.add(new VarcharField("testPersistVarchar", 12, false, table));

        var byteOS = new ByteArrayOutputStream();
        var out = new JBBPBitOutputStream(byteOS);
        try {
            out.writeString("testPersistTable", JBBPByteOrder.BIG_ENDIAN);
            out.writeString("NORMAL", JBBPByteOrder.BIG_ENDIAN);
            out.writeLong(-1, JBBPByteOrder.BIG_ENDIAN);
            out.writeInt(3, JBBPByteOrder.BIG_ENDIAN);
            out.writeString("testPersistInt", JBBPByteOrder.BIG_ENDIAN);
            out.writeString("INT", JBBPByteOrder.BIG_ENDIAN);
            out.writeBytes(new byte[]{0}, 1, JBBPByteOrder.BIG_ENDIAN);
            out.writeString("testPersistFloat", JBBPByteOrder.BIG_ENDIAN);
            out.writeString("FLOAT", JBBPByteOrder.BIG_ENDIAN);
            out.writeBytes(new byte[]{0}, 1, JBBPByteOrder.BIG_ENDIAN);
            out.writeString("testPersistVarchar", JBBPByteOrder.BIG_ENDIAN);
            out.writeString("VARCHAR", JBBPByteOrder.BIG_ENDIAN);
            out.writeBytes(new byte[]{0}, 1, JBBPByteOrder.BIG_ENDIAN);
            out.writeInt(12, JBBPByteOrder.BIG_ENDIAN);
        } catch (IOException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

        assertArrayEquals(new byte[0], table.getRecordStorage().getMetadata(context));
        table.persist(TransactionContext.empty());
        assertArrayEquals(byteOS.toByteArray(), table.getRecordStorage().getMetadata(context));

    }

    Table createTestTable(String prefix) {
        var fieldList = new ArrayList<BaseField>();
        var table = new Table(prefix + "Table", fieldList);

        // 增加测试表字段
        var intField = new IntField(prefix + "age", false, table);
        var floatField = new FloatField(prefix + "balance", false, table);
        var varcharField = new VarcharField(prefix + "name", 20, false, table);
        fieldList.add(intField);
        fieldList.add(floatField);
        fieldList.add(varcharField);

        // 创建字段索引
        try {
            intField.createIndex();
            floatField.createIndex();
        } catch (IndexAlreadyExistException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

        return table;
    }

    @Test
    public void testCURD() {

        var prefix = "testCURD";

        // 创建待测试表
        var context = TransactionContext.empty();
        var table = createTestTable(prefix);

        // 添加记录
        var data = new ArrayList<String>();
        data.add("33");
        data.add("1.2");
        data.add("'test varchar'");

        var data2 = new ArrayList<String>();
        data2.add("1");
        data2.add("-0.4");
        data2.add("'ya test varchar'");

        // 插入记录
        try {
            table.insert(context, data);
            table.insert(context, data2);
        } catch (TableConflictException | TableExistenceException | ArgumentException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

        try {
            // 查询记录，测试插入情况与查询情况
            var iter = table.searchFirst(prefix + "age", "0");
            Assert.assertTrue(iter.hasNext());
            var pair1 = iter.next();
            Assert.assertTrue(iter.hasNext());
            var pair2 = iter.next();
            Assert.assertFalse(iter.hasNext());
            Assert.assertEquals(1L, pair1.getKey());
            Assert.assertEquals(33L, pair2.getKey());

            var res1 = table.read(context, pair1.getUuid());
            var res2 = table.read(context, pair2.getUuid());

            Assert.assertTrue(res1.isPresent());
            Assert.assertTrue(res2.isPresent());

            Assert.assertEquals(1, (int) res1.get().get(0));
            Assert.assertEquals(-0.4f, res1.get().get(1));
            Assert.assertEquals("ya test varchar", (String) res1.get().get(2));

            Assert.assertEquals(33, (int) res2.get().get(0));
            Assert.assertEquals(1.2f, res2.get().get(1));
            Assert.assertEquals("test varchar", (String) res2.get().get(2));

            // 测试删除记录
            table.delete(context, pair2.getUuid());

            var iter2 = table.searchFirst(prefix + "age", "33");
            Assert.assertTrue(iter2.hasNext());
            var pair3 = iter2.next();
            Assert.assertEquals(33L, pair3.getKey());

            var res3 = table.read(context, pair3.getUuid());
            Assert.assertTrue(res3.isEmpty()); // 记录不存在

            // 测试更新记录
            table.update(context, pair1.getUuid(), data);

            var iter3 = table.searchFirst(prefix + "age", "33");
            Assert.assertTrue(iter3.hasNext());
            var pair4 = iter3.next();
            Assert.assertTrue(iter3.hasNext());
            var pair5 = iter3.next();

            // 有效的被更新记录
            Assert.assertEquals(33L, pair4.getKey());
            var res4 = table.read(context, pair4.getUuid());
            Assert.assertTrue(res4.isPresent());

            // 四记录
            Assert.assertEquals(33L, pair5.getKey());
            var res5 = table.read(context, pair5.getUuid());
            Assert.assertTrue(res5.isEmpty());

            // 测试记录是否被更新
            Assert.assertEquals(33, (int) res4.get().get(0));
            Assert.assertEquals(1.2f, res4.get().get(1));
            Assert.assertEquals("test varchar", (String) res4.get().get(2));


        } catch (TableExistenceException | TableConflictException | RecordNotFoundException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

    }

    void addTestData(TransactionContext context, Table table) throws TableConflictException, TableExistenceException, ArgumentException {
        table.insert(context, new ArrayList<>() {{
            add("1");
            add("1.2");
            add("'test varchar'");
        }});
        table.insert(context, new ArrayList<>() {{
            add("2");
            add("1.2");
            add("'test varchar'");
        }});
        table.insert(context, new ArrayList<>() {{
            add("3");
            add("1.2");
            add("'test varchar'");
        }});
        table.insert(context, new ArrayList<>() {{
            add("3");
            add("1.2");
            add("'test varchar'");
        }});
        table.insert(context, new ArrayList<>() {{
            add("4");
            add("1.2");
            add("'test varchar'");
        }});
        table.insert(context, new ArrayList<>() {{
            add("5");
            add("1.2");
            add("'test varchar'");
        }});
        table.insert(context, new ArrayList<>() {{
            add("6");
            add("1.2");
            add("'test varchar'");
        }});
        table.insert(context, new ArrayList<>() {{
            add("7");
            add("1.2");
            add("'test varchar'");
        }});
        table.insert(context, new ArrayList<>() {{
            add("7");
            add("1.2");
            add("'test varchar'");
        }});
        table.insert(context, new ArrayList<>() {{
            add("8");
            add("1.2");
            add("'test varchar'");
        }});
    }

    @Test
    public void testSearch() {

        var prefix = "testSearch";

        // 创建待测试表
        var context = TransactionContext.empty();
        var table = createTestTable(prefix);

        // 添加记录
        try {
            addTestData(context, table);
        } catch (TableConflictException | TableExistenceException | ArgumentException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

        // 查询记录
        // 测试search
        try {
            var uuids = table.search(prefix + "age", "7");
            var resList = new ArrayList<List<Object>>();
            for (var uuid : uuids) {
                var res = table.read(context, uuid);
                res.ifPresent(resList::add);
            }
            Assert.assertEquals(7, (int) resList.get(0).get(0));
            Assert.assertEquals(1.2f, resList.get(0).get(1));
            Assert.assertEquals("test varchar", (String) resList.get(0).get(2));

            Assert.assertEquals(7, (int) resList.get(1).get(0));
            Assert.assertEquals(1.2f, resList.get(1).get(1));
            Assert.assertEquals("test varchar", (String) resList.get(1).get(2));

        } catch (TableExistenceException | TableConflictException | RecordNotFoundException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

    }

    @Test
    public void testSearchAll() {

        var prefix = "testSearchAll";

        // 创建待测试表
        var context = TransactionContext.empty();
        var table = createTestTable(prefix);

        // 添加记录
        try {
            addTestData(context, table);
        } catch (TableConflictException | TableExistenceException | ArgumentException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }


        // 测试searchAll
        try {
            var uuids = table.searchAll(prefix + "age");
            var resList = new ArrayList<List<Object>>();
            while (uuids.hasNext()) {
                var uuid = uuids.next();
                table.read(context, uuid.getUuid()).ifPresent(resList::add);
            }

            Assert.assertEquals(1, (int) resList.get(0).get(0));
            Assert.assertEquals(1.2f, resList.get(0).get(1));
            Assert.assertEquals("test varchar", (String) resList.get(0).get(2));

            Assert.assertEquals(2, (int) resList.get(1).get(0));
            Assert.assertEquals(1.2f, resList.get(1).get(1));
            Assert.assertEquals("test varchar", (String) resList.get(1).get(2));

            Assert.assertEquals(3, (int) resList.get(2).get(0));
            Assert.assertEquals(1.2f, resList.get(2).get(1));
            Assert.assertEquals("test varchar", (String) resList.get(2).get(2));

            Assert.assertEquals(3, (int) resList.get(3).get(0));
            Assert.assertEquals(1.2f, resList.get(3).get(1));
            Assert.assertEquals("test varchar", (String) resList.get(3).get(2));

            Assert.assertEquals(4, (int) resList.get(4).get(0));
            Assert.assertEquals(1.2f, resList.get(4).get(1));
            Assert.assertEquals("test varchar", (String) resList.get(4).get(2));

            Assert.assertEquals(5, (int) resList.get(5).get(0));
            Assert.assertEquals(1.2f, resList.get(5).get(1));
            Assert.assertEquals("test varchar", (String) resList.get(5).get(2));

            Assert.assertEquals(6, (int) resList.get(6).get(0));
            Assert.assertEquals(1.2f, resList.get(6).get(1));
            Assert.assertEquals("test varchar", (String) resList.get(6).get(2));

            Assert.assertEquals(7, (int) resList.get(7).get(0));
            Assert.assertEquals(1.2f, resList.get(7).get(1));
            Assert.assertEquals("test varchar", (String) resList.get(7).get(2));

            Assert.assertEquals(7, (int) resList.get(8).get(0));
            Assert.assertEquals(1.2f, resList.get(8).get(1));
            Assert.assertEquals("test varchar", (String) resList.get(8).get(2));

            Assert.assertEquals(8, (int) resList.get(9).get(0));
            Assert.assertEquals(1.2f, resList.get(9).get(1));
            Assert.assertEquals("test varchar", (String) resList.get(9).get(2));


        } catch (TableExistenceException | TableConflictException | RecordNotFoundException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }
    }

    @Test
    public void testSearchFirst() {
        var prefix = "testSearchFirst";

        // 创建待测试表
        var context = TransactionContext.empty();
        var table = createTestTable(prefix);

        // 添加记录
        try {
            addTestData(context, table);
        } catch (TableConflictException | TableExistenceException | ArgumentException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }
        // 测试searchFirst
        try {
            var uuids = table.searchFirst(prefix + "age", "3");
            var resList = new ArrayList<List<Object>>();
            while (uuids.hasNext()) {
                var uuid = uuids.next();
                table.read(context, uuid.getUuid()).ifPresent(resList::add);
            }

            Assert.assertEquals(3, (int) resList.get(0).get(0));
            Assert.assertEquals(1.2f, resList.get(0).get(1));
            Assert.assertEquals("test varchar", (String) resList.get(0).get(2));

            Assert.assertEquals(3, (int) resList.get(1).get(0));
            Assert.assertEquals(1.2f, resList.get(1).get(1));
            Assert.assertEquals("test varchar", (String) resList.get(1).get(2));

            Assert.assertEquals(4, (int) resList.get(2).get(0));
            Assert.assertEquals(1.2f, resList.get(2).get(1));
            Assert.assertEquals("test varchar", (String) resList.get(2).get(2));

        } catch (TableExistenceException | TableConflictException | RecordNotFoundException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }
    }

    @Test
    public void testSearchFirstNotEqual() {
        var prefix = "testSearchFirstNotEqual";

        // 创建待测试表
        var context = TransactionContext.empty();
        var table = createTestTable(prefix);

        // 添加记录
        try {
            addTestData(context, table);
        } catch (TableConflictException | TableExistenceException | ArgumentException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

        // 测试searchFirstNotEqual
        try {
            var uuids = table.searchFirstNotEqual(prefix + "age", "3");
            var resList = new ArrayList<List<Object>>();
            while (uuids.hasNext()) {
                var uuid = uuids.next();
                table.read(context, uuid.getUuid()).ifPresent(resList::add);
            }

            Assert.assertEquals(4, (int) resList.get(0).get(0));
            Assert.assertEquals(1.2f, resList.get(0).get(1));
            Assert.assertEquals("test varchar", (String) resList.get(0).get(2));

        } catch (TableExistenceException | TableConflictException | RecordNotFoundException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }
    }

    @Test
    public void testCheckStringEntry() {
        var passEntry = new ArrayList<String>();
        passEntry.add("33");
        passEntry.add("1.2");
        passEntry.add("'test varchar'");

        var failEntry = new ArrayList<String>();
        failEntry.add("33");
        failEntry.add("'test varchar'");
        failEntry.add("1.2");

        var fieldList = new ArrayList<BaseField>();
        var table = new Table("testCheckStringEntryTable", fieldList);
        var intField = new IntField("testCheckStringEntryInt", false, table);
        var floatField = new FloatField("testCheckStringEntryFloat", false, table);
        var varcharField = new VarcharField("testCheckStringEntryVarchar", 20, false, table);
        fieldList.add(intField);
        fieldList.add(floatField);
        fieldList.add(varcharField);

        Assert.assertTrue(table.checkStringEntry(passEntry));
        Assert.assertFalse(table.checkStringEntry(failEntry));

    }

    @Test
    public void testStringEntryToBytes() {
        var passEntry = new ArrayList<String>();
        passEntry.add("33");
        passEntry.add("1.2");
        passEntry.add("'test varchar'");

        var failEntry = new ArrayList<String>();
        failEntry.add("33");
        failEntry.add("'test varchar'");
        failEntry.add("1.2");

        var fieldList = new ArrayList<BaseField>();
        var table = new Table("testStringEntryToBytesTable", fieldList);
        var intField = new IntField("testStringEntryToBytesInt", false, table);
        var floatField = new FloatField("testStringEntryToBytesFloat", false, table);
        var varcharField = new VarcharField("testStringEntryToBytesVarchar", 20, false, table);
        fieldList.add(intField);
        fieldList.add(floatField);
        fieldList.add(varcharField);

        try {
            var bytes = table.stringEntryToBytes(passEntry);
            var expected = new byte[]{
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
            assertArrayEquals(expected, bytes);
        } catch (TableConflictException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

        try {
            table.stringEntryToBytes(failEntry);
            Assert.fail("");
        } catch (TableConflictException e) {
            log.error("Exception expected: ", e);
        }
    }

    @Test
    public void testParseEntry() {
        var passEntry = new byte[]{
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

        var failEntry = new byte[]{
                0,
                0, 0, 0, 33, // int 33
                0,
                12,
                116, 101, 115, 116,
                32, 118, 97, 114,
                99, 104, 97, 114, // varchar test varchar
                0,
                63, -103, -103, -102, // float 1.2
        };

        var fieldList = new ArrayList<BaseField>();
        var table = new Table("testParseEntryTable", fieldList);
        var intField = new IntField("testParseEntryInt", false, table);
        var floatField = new FloatField("testParseEntryFloat", false, table);
        var varcharField = new VarcharField("testParseEntryVarchar", 20, false, table);
        fieldList.add(intField);
        fieldList.add(floatField);
        fieldList.add(varcharField);

        try {
            var list = table.parseEntry(passEntry);
            Assert.assertEquals(33, (int) list.get(0));
            Assert.assertEquals(1.2f, list.get(1));
            Assert.assertEquals("test varchar", (String) list.get(2));
        } catch (TableConflictException | IOException e) {
            log.error("Exception expected: ", e);
            Assert.fail();
        }

        try {
            table.parseEntry(failEntry);
            Assert.fail();
        } catch (TableConflictException | IOException e) {
            log.error("Exception expected: ", e);
        }

    }


}
