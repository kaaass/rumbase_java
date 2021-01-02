package net.kaaass.rumbase.table.mock;

import net.kaaass.rumbase.table.FieldType;
import net.kaaass.rumbase.table.FieldValue;
import net.kaaass.rumbase.table.IField;
import net.kaaass.rumbase.table.ITable;

import java.io.*;
import java.util.*;

/**
 * Mock字段结构，仅用于测试
 *
 * @author @KveinAxel
 */
@Deprecated
public class MockField implements IField {

    private String name;

    private FieldType type;

    private UUID selfUUID;

    private ITable table;

    private UUID index;
//    private BTree bTree;


    /**
     * 内存中创建的Mock存储
     */
    private static final Map<String, MockField> MOCK_STORAGES = new HashMap<>();

    private final String mockId;

    private final Map<UUID, byte[]> memoryStorage = new HashMap<>();

    private MockField(String mockId, String name, FieldType fieldType) {
        this.mockId = mockId;
        index = null;
        type = fieldType;
        this.name = name;
    }

    @Override
    public void parseSelf(byte[] raw) {

    }

    @Override
    public byte[] persistSelf() {
        return new byte[0];
    }

    @Override
    public Boolean isIndexed() {
        return index != null;
    }

    @Override
    public FieldType getType() {
        return type;
    }

    @Override
    public int getSize() {
        return switch (type) {
            case INT -> 4;
            case FLOAT -> 4;
        };
    }

    @Override
    public List<UUID> searchRange(FieldValue left, FieldValue right) {
        if (left.compareTo(right) >= 0) {
            return new ArrayList<>();
        }
        return null;
    }

    @Override
    public Optional<UUID> search(FieldValue value) {
        return Optional.empty();
    }


    @Override
    public Optional<FieldValue> strToValue(String valStr) {
        return switch (type) {
            case INT -> {
                int value;
                try {
                    value = Integer.parseInt(valStr);
                } catch (NumberFormatException e) {
                    yield Optional.empty();
                }

                var fieldValue = new FieldValue<Integer>(type);
                fieldValue.setValue(value);

                yield Optional.of(fieldValue);
            }
            case FLOAT -> {
                float value;
                try {
                    value = Float.parseFloat(valStr);
                } catch (NumberFormatException e) {
                    yield Optional.empty();
                }
                var fieldValue = new FieldValue<Float>(type);
                fieldValue.setValue(value);

                yield Optional.of(fieldValue);
            }
        };
    }

    @Override
    public Optional<byte[]> valueToRaw(FieldValue value) {
        byte[] bytes;
        try (
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(bos)
        ) {
            bytes = switch (type) {
                case INT -> {
                    int intVal = (int) value.getValue();
                    out.writeInt(intVal);
                    out.close();

                    yield bos.toByteArray();
                }
                case FLOAT -> {
                    float floatVal = (float) value.getValue();

                    out.writeFloat(floatVal);
                    out.close();

                    yield bos.toByteArray();
                }
            };
        } catch (Exception e) {
            return Optional.empty();
        }
        return Optional.of(bytes);
    }

    @Override
    public Optional<FieldValue> rawToValue(byte[] raw) {
        Optional<FieldValue> value;
        try (
                var bais = new ByteArrayInputStream(raw);
                var dis = new DataInputStream(bais)
                ) {
            value = switch (type) {
                case INT -> {
                    var fieldValue = new FieldValue<Integer>(FieldType.INT);
                    if (raw.length != fieldValue.getType().getSize()) {
                        yield Optional.empty();
                    }
                    fieldValue.setValue(dis.readInt());
                    yield Optional.of(fieldValue);
                }
                case FLOAT -> {
                    var fieldValue = new FieldValue<Float>(FieldType.FLOAT);
                    fieldValue.setValue(dis.readFloat());
                    yield Optional.of(fieldValue);
                }
            };
        } catch (Exception e) {
            return Optional.empty();
        }
        return value;
    }

    @Override
    public String getFieldName() {
        return name;
    }

    public static MockField ofFile(String filepath, String name, FieldType fieldType) {
        var mockId = "file" + filepath;
        if (MOCK_STORAGES.containsKey(mockId)) {
            return MOCK_STORAGES.get(mockId);
        }
        var result = new MockField(mockId, name, fieldType);
        MOCK_STORAGES.put(mockId, result);
        return result;
    }

    public String getMockId() {
        return mockId;
    }
}
