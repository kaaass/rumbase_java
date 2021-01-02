package net.kaaass.rumbase.table.exception;

import net.kaaass.rumbase.exception.RumbaseException;

import java.util.HashMap;
import java.util.Map;

/**
 * E3002 类型不匹配异常
 * <p>
 * E3002-1 字段类型不匹配
 * E3002-2 字段类型不存在
 * E3002-3 Entry类型不匹配
 *
 * @author @KveinAxel
 */
public class TypeIncompatibleException extends RumbaseException {
    public static final Map<Integer, String> REASONS = new HashMap<>(){{
        put(1, "字段类型不匹配");
        put(2, "字段类型不存在");
        put(3, "Entry不匹配");
    }};

    /**
     * 类型不匹配异常
     *
     * @param subId  子错误号
     */
    public TypeIncompatibleException(int subId) {
        super(3002, subId, REASONS.get(subId));
    }
}
