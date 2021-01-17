package net.kaaass.rumbase.query.exception;

import net.kaaass.rumbase.exception.RumbaseException;

import java.util.HashMap;
import java.util.Map;

/**
 * E2001 参数异常
 * <p>
 * E2001-1 列参数异常
 * <p>
 * E2001-2 请求不包含索引列
 *
 * @author @KveinAxel
 */
public class ArgumentException extends RumbaseException {
    public static final Map<Integer, String> REASONS = new HashMap<>(){{
        put(1, "列参数异常");
        put(2, "请求不包含索引列");
    }};

    /**
     * 类型不匹配异常
     *
     * @param subId  子错误号
     */
    public ArgumentException(int subId) {
        super(2001, subId, REASONS.get(subId));
    }
}
