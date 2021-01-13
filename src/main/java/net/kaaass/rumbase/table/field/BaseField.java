package net.kaaass.rumbase.table.field;

import lombok.*;
import net.kaaass.rumbase.table.exception.TableConflictException;

import java.io.InputStream;
import java.io.OutputStream;


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
public abstract class BaseField {

    /**
     * 字段名
     */
    @Getter
    @NonNull
    protected final String name;

    /**
     * 字段类型
     */
    @Getter
    @NonNull
    private final FieldType type;

    /**
     * 判断字符串是否能够转成符合当前字段约束的值
     * @param valStr 待检查字符串
     * @return 满足情况
     */
    public abstract boolean checkStr(String valStr);

    /**
     * 将字符串转成哈希
     * @param str 待转换字符串
     * @return 哈希
     */
    public abstract long strToHash(String str) throws TableConflictException;

    /**
     * 从输入流中反序列化出一个满足当前字段约束的值对象
     * @param inputStream 输入流
     * @throws TableConflictException 输入流中读出对象与字段类型不匹配
     * @return 值对象
     */
    public abstract Object deserialize(InputStream inputStream) throws TableConflictException;

    /**
     * 从输入流中反序列化一个满足当前类型的对象，并判断是否满足约束
     * @param inputStream 输入流
     * @return 满足情况
     */
    public abstract boolean checkInputStream(InputStream inputStream);

    /**
     * 将值对象序列化到输出流中
     * @param outputStream 输出流
     * @param strVal 值对象
     * @throws TableConflictException 类型不匹配
     */
    public abstract void serialize(OutputStream outputStream, String strVal) throws TableConflictException;

    /**
     * 向索引插入一个键值对
     * @param value 值对象
     * @param uuid uuid
     */
    public abstract void insertIndex(Object value, long uuid);

    // todo index查询
}