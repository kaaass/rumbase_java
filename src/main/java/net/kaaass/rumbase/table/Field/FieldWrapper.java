package net.kaaass.rumbase.table.Field;

import com.igormaznitsa.jbbp.io.JBBPOut;
import com.igormaznitsa.jbbp.model.JBBPFieldStruct;
import net.kaaass.rumbase.table.Field.Visitor.*;
import net.kaaass.rumbase.table.exception.NotFoundException;
import net.kaaass.rumbase.table.exception.TypeIncompatibleException;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

public interface FieldWrapper {

    interface Cases<X> {
        X floatField(FloatField floatField) throws TypeIncompatibleException, NotFoundException;

        X intField(IntField intField) throws TypeIncompatibleException, NotFoundException;

        X varcharField(VarcharField varcharField) throws TypeIncompatibleException, NotFoundException;
    }

    <X> X match(Cases<X> cases) throws TypeIncompatibleException, NotFoundException;

    default Field asFieldValue() throws TypeIncompatibleException, NotFoundException {
        return match(new AsFieldValueVisitor());
    }

    static FieldWrapper fromField(Field field) {
        return new FromFieldVisitor(field).getFieldWrapper();
    }

    /**
     * 从配置信息的字节数组获取字段的属性
     *
     * @param raw 配置信息
     */
    public static void useParseSelfVisitor(Field field, byte[] raw) {
        // todo
    }

    /**
     * 将自身的配置信息持久化成字节数组
     *
     * @return 配置信息
     */
    public static byte[] usePersistSelfVisitor(Field field) {
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
    public static List<UUID> useSearchRangeVisitor(Field field, Field left, Field right) throws TypeIncompatibleException, NotFoundException {
        return fromField(field).match(new SearchRangeVisitor(left, right));
    }

    /**
     * 在该字段的索引上进行单点搜索
     *
     * @param value 字段的值
     * @return 字段的uuid的Optional
     */
    public static Optional<UUID> useSearchOptionalVisitor(Field field, Field value) throws TypeIncompatibleException, NotFoundException {
        return fromField(field).match(new SearchOptionalVisitor(value));
    }

    /**
     * 在该字段的索引上进行单点搜索
     *
     * @param value 字段的值
     * @return 字段的uuid
     * @throws NotFoundException 未查找到对应值的元组
     */
    public static UUID useSearchVisitor(Field field, Field value) throws NotFoundException, TypeIncompatibleException {
        return fromField(field).match(new SearchVisitor(value));
    }

    /**
     * 将字符串转换成字段值
     *
     * @param valStr 字符串
     * @return 字段值的Optional
     */
    public static Field useStrToValueVisitor(Field field, String valStr) throws TypeIncompatibleException, NotFoundException {
        return fromField(field).match(new StrToValueVisitor(valStr));
    }

    /**
     * 将字段值转成字节数组
     *
     * @param value 字段值
     * @return 字节数组
     */
    public static byte[] useValueToRawVisitor(Field field, Field value) throws TypeIncompatibleException, NotFoundException {
        return fromField(field).match(new ValueToRawVisitor(value));
    }

    /**
     * 将字节数组转成字段值
     *
     * @param struct JBBP结构体
     * @return 字段值
     */
    public static Field useJBBPStructToValueVisitor(Field field, JBBPFieldStruct struct) throws TypeIncompatibleException, NotFoundException {
        return fromField(field).match(new JBBPStructToValueVisitor(struct, field.getName()));
    }

    /**
     * 检查值的字符串是否满足当前字段的约束
     *
     * @param valStr 值字符串
     * @return 满足情况
     */
    public static boolean useCheckStrVisitor(Field field, String valStr) throws TypeIncompatibleException, NotFoundException {
        // fixme 目前仅检查了字段类型，没有检查字段约束
        return fromField(field).match(new CheckStrVisitor(valStr));
    }

    public static boolean useCheckValueVisitor(Field field, Field value) throws TypeIncompatibleException, NotFoundException {
        // fixme 目前仅检查了字段类型，没有检查字段约束
        return fromField(field).match(new CheckValueVisitor(value));
    }



    public static void useAppendToJBBPOutVisitor(Field field, JBBPOut jbbpOut) throws TypeIncompatibleException, NotFoundException {
        fromField(field).match(new AppendToJBBPOutVisitor(jbbpOut));
    }
}