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
        if (type == FieldType.INT) {
            return 4;
        } else if (type == FieldType.FLOAT) {
            return 4;
        }
        return 4;
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
        if (type == FieldType.INT) {
            try {
                var value = Integer.parseInt(valStr);
                var fieldValue = new FieldValue<Integer>(type);

                fieldValue.setValue(value);

                return Optional.of(fieldValue);
            } catch (NumberFormatException e) {
                return Optional.empty();
            }

        } else if (type == FieldType.FLOAT) {
            try {
                var value = Float.parseFloat(valStr);
                var fieldValue = new FieldValue<Float>(type);

                fieldValue.setValue(value);

                return Optional.of(fieldValue);
            } catch (NumberFormatException e) {
                return Optional.empty();
            }
        }

        return Optional.empty();
    }

    @Override
    public Optional<byte[]> valueToRaw(FieldValue value) {
        try (
                ByteArrayOutputStream bos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(bos)
        ) {
            if (type == FieldType.INT) {
                int intVal = (int) value.getValue();

                out.writeInt(intVal);
                out.close();

                return Optional.of(bos.toByteArray());

            } else if (type == FieldType.FLOAT) {
                float floatVal = (float) value.getValue();

                out.writeFloat(floatVal);
                out.close();

                return Optional.of(bos.toByteArray());
            }

        } catch (Exception e) {
            return Optional.empty();
        }
        return Optional.empty();
    }

    @Override
    public Optional<FieldValue> rawToValue(byte[] raw) {
        try (
                var bais = new ByteArrayInputStream(raw);
                var dis = new DataInputStream(bais)
                ) {
            if (type == FieldType.INT) {
                var fieldValue = new FieldValue<Integer>(FieldType.INT);

                if (raw.length != fieldValue.getType().getSize()) {
                    return Optional.empty();
                }

                fieldValue.setValue(dis.readInt());
                return Optional.of(fieldValue);

            } else if (type == FieldType.FLOAT) {
                var fieldValue = new FieldValue<Float>(FieldType.FLOAT);

                if (raw.length != fieldValue.getType().getSize()) {
                    return Optional.empty();
                }

                fieldValue.setValue(dis.readFloat());
                return Optional.of(fieldValue);
            }

        } catch (Exception e) {
            return Optional.empty();
        }
        return Optional.empty();
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
