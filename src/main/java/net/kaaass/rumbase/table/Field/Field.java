package net.kaaass.rumbase.table.Field;

import lombok.*;
import net.kaaass.rumbase.table.Field.Visitor.FieldVisitor;
import net.kaaass.rumbase.table.exception.TypeIncompatibleException;

import java.util.UUID;

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
public abstract class Field<T> implements Comparable<Field>  {

    @Getter
    @NonNull
    protected final String name;

    @Getter
    @NonNull
    private final FieldType type;

    @Getter
    @Setter
    private UUID selfUUID = null;

    @Getter
    @Setter
    private UUID index = null;

    /**
     * 如果是字符数组要求长度在 0 ~ 2^15 - 1 范围内
     */
    @Getter
    @Setter
    @NonNull
    protected T value;

    public abstract void accept(FieldVisitor visitor);

    /**
     * 检查字段值是否满足当前字段的约束
     *
     * @param field 待检查字段值
     * @return 满足情况
     */
    public boolean checkValueVisitor(Field field) {
        // fixme 目前仅检查了类型是否匹配，没有检查其他约束
        return field != null && type == field.getType();
    }

    public abstract String getPrepareCode() throws TypeIncompatibleException;
    }
