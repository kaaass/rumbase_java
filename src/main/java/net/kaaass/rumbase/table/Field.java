package net.kaaass.rumbase.table;

import com.igormaznitsa.jbbp.model.*;
import lombok.*;
import net.kaaass.rumbase.table.exception.NotFoundException;
import net.kaaass.rumbase.table.exception.TypeIncompatibleException;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.igormaznitsa.jbbp.io.JBBPOut.BeginBin;

/**
 * 字段结构
 * <p>
 * 提供字段解析服务
 * <p>
 * 提供字段的索引处理
 *
 * @author @KveinAxel
 */
@RequiredArgsConstructor
public class Field {

    @Getter
    @NonNull
    private final String name;

    @Getter
    @NonNull
    private final FieldType type;

    @Getter
    @Setter
    private UUID selfUUID = null;

    @NonNull
    private final Table table;

    @Getter
    @Setter
    private UUID index = null;

    /**
     * 从配置信息的字节数组获取字段的属性
     *
     * @param raw 配置信息
     */
    public void parseSelf(byte[] raw) {
        // todo
    }

    /**
     * 将自身的配置信息持久化成字节数组
     *
     * @return 配置信息
     */
    public byte[] persistSelf() {
        // todo
        return new byte[0];

    }

    /**
     * 在该字段的索引上进行范围搜索
     *
     * @param left  查询左端点
     * @param right 查询右端点
     * @return 查询到的uuid
     */
    public List<UUID> searchRange(FieldValue left, FieldValue right) {
        // todo
        return null;
    }

    /**
     * 在该字段的索引上进行单点搜索
     *
     * @param value 字段的值
     * @return 字段的uuid的Optional
     */
    public Optional<UUID> searchOptional(FieldValue value) {
        // todo
        return Optional.empty();
    }

    /**
     * 在该字段的索引上进行单点搜索
     *
     * @param value 字段的值
     * @return 字段的uuid
     * @throws NotFoundException 未查找到对应值的元组
     */
    public UUID search(FieldValue value) throws NotFoundException {
        // todo
        return UUID.randomUUID();
    }

    /**
     * 将字符串转换成字段值
     *
     * @param valStr 字符串
     * @return 字段值的Optional
     */
    public FieldValue strToValue(String valStr) throws TypeIncompatibleException {
        FieldValue fieldValue;
        switch (type) {
            case INT: {
                var value = Integer.parseInt(valStr);
                return new FieldValue<Integer>(value, type);
            }
            case FLOAT: {
                var value = Float.parseFloat(valStr);
                return new FieldValue<Float>(value, type);
            }
            case VARCHAR: {
                var value = valStr.toCharArray();
                return new FieldValue<char[]>(value, type);
            }
        }

        throw new TypeIncompatibleException(2);
    }

    /**
     * 将字段值转成字节数组
     *
     * @param value 字段值
     * @return 字节数组
     */
    public byte[] valueToRaw(FieldValue value) throws IOException, TypeIncompatibleException {

        switch (type) {
            case INT: {
                return BeginBin().Bit((int) value.getValue()).End().toByteArray();

            }
            case FLOAT: {
                return BeginBin().Bin((float) value.getValue()).End().toByteArray();

            }
            case VARCHAR: {
                char[] v = (char[]) value.getValue();
                return BeginBin().Bin((short) v.length).Bin(v).End().toByteArray();
            }
        }


        throw new TypeIncompatibleException(2);
    }

    public String getPrepareCode() throws TypeIncompatibleException {
        switch (type) {
            case INT: {
                return "int " + name + ";";
            }
            case FLOAT: {
                return "float " + name + ";";
            }
            case VARCHAR: {
                return "short " + name + "Len;ubyte[" + name + "Len] " + name + ";";
            }
        }

        throw new TypeIncompatibleException(2);
    }

    /**
     * 将字节数组转成字段值
     *
     * @param struct JBBP结构体
     * @return 字段值
     */
    public FieldValue JBBPStructToValue(JBBPFieldStruct struct) throws TypeIncompatibleException {

        switch (type) {
            case INT: {
                var val = struct.findFieldForNameAndType(name, JBBPFieldInt.class).getAsInt();
                return new FieldValue<>(val, type);
            }
            case FLOAT: {
                var val = struct.findFieldForNameAndType(name, JBBPFieldFloat.class).getAsFloat();
                return new FieldValue<>(val, type);
            }
            case VARCHAR: {
                var bytes = struct.findFieldForNameAndType(name, JBBPFieldArrayUByte.class).getArray();
                var charArray = new String(bytes, StandardCharsets.US_ASCII).toCharArray();
                return new FieldValue<>(charArray, FieldType.VARCHAR);
            }
        }

        throw new TypeIncompatibleException(2);
    }

    /**
     * 检查值的字符串是否满足当前字段的约束
     *
     * @param valStr 值字符串
     * @return 满足情况
     */
    public boolean checkStr(String valStr) {
        // fixme 目前仅检查了字段类型，没有检查字段约束
        try {

            switch (type) {
                case INT: {
                    Integer.parseInt(valStr);
                }
                case FLOAT: {
                    Float.parseFloat(valStr);
                }
                case VARCHAR: { }
            }

            return true;
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 检查字段值是否满足当前字段的约束
     *
     * @param fieldValue 待检查字段值
     * @return 满足情况
     */
    public boolean checkValue(FieldValue fieldValue) {
        // fixme 目前仅检查了类型是否匹配，没有检查其他约束
        return fieldValue != null && type == fieldValue.getType();
    }

}
