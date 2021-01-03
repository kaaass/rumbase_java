//package net.kaaass.rumbase.table;
//
//import junit.framework.TestCase;
//
//import java.util.ArrayList;
//
///**
// * 表结构测试
// *
// * @author @KveinAxel
// * @see Table
// */
//public class TableTest extends TestCase {
//    public void testParseSelf() {
//        // todo
//    }
//
//    public void testPersistSelf() {
//        // todo
//    }
//
//    public void testDelete() {
//        // todo
//    }
//
//    public void testUpdate() {
//        // todo
//    }
//
//    public void testRead() {
//        // todo
//    }
//
//    public void testInsert() {
//        // todo
//    }
//
//    public void testSearch() {
//        // todo
//    }
//
//    public void testStrToEntry() {
//        var str = new ArrayList<String>();
//        str.add("1");
//        str.add("1.2");
//        str.add("3");
//
//        var fields = new ArrayList<Field>();
//        fields.add(MockField.ofFile("id", "id", FieldType.INT));
//        fields.add(MockField.ofFile("f", "f", FieldType.FLOAT));
//        fields.add(MockField.ofFile("i", "i", FieldType.INT));
//        var table = new MockTable("mockTable", fields);
//
//        var res = table.strToEntry(str);
//        assertTrue("compatible fields can be transformed", res.isPresent());
//        var entry = res.get();
//        assertEquals("compatible fields can be transformed properly", entry.get("id").getValue(), 1);
//        assertEquals("compatible fields can be transformed properly", entry.get("f").getValue(), 1.2f);
//        assertEquals("compatible fields can be transformed properly", entry.get("i").getValue(), 3);
//        assertNull("dummy fields can not exist", entry.get("dummy"));
//
//        str.add("4");
//        var res2 = table.strToEntry(str);
//        assertTrue("incompatible type can't be transformed properly", res2.isEmpty());
//    }
//
//    public void testParseEntry() {
//        var fields = new ArrayList<Field>();
//        fields.add(MockField.ofFile("id", "id", FieldType.INT));
//        fields.add(MockField.ofFile("f", "f", FieldType.FLOAT));
//        fields.add(MockField.ofFile("i", "i", FieldType.INT));
//        var table = new MockTable("mockTable", fields);
//
//        var bytes = new byte[]{
//                0x00, 0x00, 0x00, 0x01, 0x3f,
//                (byte) 0x99, (byte) 0x99, (byte) 0x9a,
//                0x00, 0x00, 0x00, 0x03
//        };
//
//        var res = table.parseEntry(bytes);
//        assertTrue("compatible fields can be transformed properly", res.isPresent());
//        var entry = res.get();
//        assertEquals("compatible fields can be transformed properly", 1, entry.get("id").getValue());
//        assertEquals("compatible fields can be transformed properly", 1.2f, entry.get("f").getValue());
//        assertEquals("compatible fields can be transformed properly", 3, entry.get("i").getValue());
//
//        var bytes2 = new byte[]{
//                0x00, 0x00, 0x00, 0x01, 0x3f,
//                (byte) 0x99, (byte) 0x99, (byte) 0x9a,
//                0x00, 0x00, 0x00, 0x03,
//                0x00, 0x00, 0x00, 0x03
//        };
//        var res2 = table.parseEntry(bytes2);
//        assertTrue("dummy fields can not exist", res2.isEmpty());
//
//
//    }
//
//
//}
