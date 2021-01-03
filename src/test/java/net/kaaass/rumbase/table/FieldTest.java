//package net.kaaass.rumbase.table;
//
//import junit.framework.TestCase;
//
//import static org.junit.Assert.assertArrayEquals;
//
///**
// * 字段结构测试
// *
// * @author @KveinAxel
// * @see Field
// */
//public class FieldTest extends TestCase {
//    public void testStrToValue() {
//
//        Field field = MockField.ofFile("test_str2value_int", "id", FieldType.INT);
//        var value = field.strToValue("1");
//        assertTrue("compatible value should parse successfully.", value.isPresent());
//        assertEquals("compatible value should be parsed in a true way", 1, value.get().getValue());
//
//        Field field2 = MockField.ofFile("test_str2value_float", "id", FieldType.FLOAT);
//        var value2 = field2.strToValue("1.2");
//        assertTrue("compatible value should be successful to parse.", value2.isPresent());
//        assertEquals("compatible value should be parsed in a proper way", 1.2f, (float) value2.get().getValue());
//
//        Field field3 = MockField.ofFile("test_str2value_failed", "id", FieldType.INT);
//        var value3 = field3.strToValue("a");
//        assertTrue("incompatible value should fail to parse", value3.isEmpty());
//    }
//
//    public void testValueToRaw() {
//        FieldValue<Integer> fieldValue = new FieldValue<>(FieldType.INT);
//        fieldValue.setValue(1);
//        Field field = MockField.ofFile("test_value2raw_int", "id", FieldType.INT);
//        var raw = field.valueToRaw(fieldValue);
//        assertTrue("compatible value should be successful to transform.", raw.isPresent());
//        assertArrayEquals("compatible value should be transform in a proper way", raw.get(), new byte[]{0x00, 0x00, 0x00, 0x01});
//
//        FieldValue<Float> fieldValue2 = new FieldValue<>(FieldType.FLOAT);
//        fieldValue2.setValue(1.2f);
//        Field field2 = MockField.ofFile("test_value2raw_float", "id", FieldType.FLOAT);
//        var raw2 = field2.valueToRaw(fieldValue2);
//        assertTrue("compatible value should be successful to transform.", raw2.isPresent());
//        assertArrayEquals("compatible value should be transform in a proper way", raw2.get(), new byte[]{0x3f, (byte) 0x99, (byte) 0x99, (byte) 0x9a});
//
//    }
//
//    public void testRawToValue() {
//        var bytes = new byte[]{0x00, 0x00, 0x00, 0x01};
//        Field field = MockField.ofFile("test_raw2value_int", "id", FieldType.INT);
//        var value = field.rawToValue(bytes);
//        assertTrue("compatible value should be successful to transform.", value.isPresent());
//        assertEquals("compatible value should be transform in a proper way", 1, (int) value.get().getValue());
//
//        var bytes2 = new byte[]{0x3f, (byte) 0x99, (byte) 0x99, (byte) 0x9a};
//        Field field2 = MockField.ofFile("test_raw2value_float", "id", FieldType.FLOAT);
//        var value2 = field2.rawToValue(bytes2);
//        assertTrue("compatible value should be successful to transform.", value2.isPresent());
//        assertEquals("compatible value should be transform in a proper way", 1.2f, (float) value2.get().getValue());
//
//        var bytes3 = new byte[]{0x00, 0x00, 0x00, 0x00, 0x01};
//        Field field3 = MockField.ofFile("test_raw2value_failed", "id", FieldType.INT);
//        var value3 = field.rawToValue(bytes3);
//        assertTrue("incompatible value should be failed to transform.", value3.isEmpty());
//
//    }
//
//    public void testSearch() {
//
//    }
//
//    public void testSearchRange() {
//        var fieldValue = new FieldValue<Integer>(FieldType.INT);
//        var fieldValue2 = new FieldValue<Integer>(FieldType.INT);
//        fieldValue.setValue(2);
//        fieldValue2.setValue(1);
//        Field field = MockField.ofFile("test_search_range_int", "id", FieldType.INT);
//
//        var res = field.searchRange(fieldValue, fieldValue2);
//        assertEquals("bad seq can only get a 0 size list", 0, res.size());
//        var res2 = field.searchRange(fieldValue2, fieldValue);
//        assertEquals("good seq can get the proper answer", null, res2);
//
//        var fieldValue3 = new FieldValue<Float>(FieldType.FLOAT);
//        var fieldValue4 = new FieldValue<Float>(FieldType.FLOAT);
//        fieldValue3.setValue(2.1f);
//        fieldValue4.setValue(1.1f);
//        Field field2 = MockField.ofFile("test_search_range_float", "id", FieldType.INT);
//
//        var res3 = field2.searchRange(fieldValue3, fieldValue4);
//        assertEquals("bad seq can only get a 0 size list", 0, res3.size());
//        var res4 = field2.searchRange(fieldValue4, fieldValue3);
//        assertEquals("good seq can get the proper answer", null, res4);
//
//        var fieldValue5 = new FieldValue<Integer>(FieldType.INT);
//        var fieldValue6 = new FieldValue<Float>(FieldType.FLOAT);
//        fieldValue5.setValue(2);
//        fieldValue6.setValue(1.1f);
//        Field field3 = MockField.ofFile("test_search_range_failed", "id", FieldType.INT);
//
//        var res5 = field3.searchRange(fieldValue5, fieldValue6);
//        assertEquals("incompatible can only get a 0 size list", 0, res5.size());
//        var res6 = field3.searchRange(fieldValue6, fieldValue5);
//        assertEquals("incompatible can only get a 0 size list", 0, res6.size());
//
//
//    }
//
//    public void testParseSelf() {
//        // todo
//    }
//
//    public void testPersistSelf() {
//        // todo
//    }
//}
