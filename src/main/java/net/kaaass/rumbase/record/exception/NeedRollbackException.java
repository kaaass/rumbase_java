package net.kaaass.rumbase.record.exception;

import net.kaaass.rumbase.exception.RumbaseRuntimeException;

import java.util.HashMap;
import java.util.Map;

/**
 * E5003 需要自动回滚异常
 * <p>
 * E5003-1 发生版本跳跃，需要自动回滚事务
 *
 * @author kaaass
 */
public class NeedRollbackException extends RumbaseRuntimeException {

    public static final Map<Integer, String> REASONS = new HashMap<>() {{
        put(1, "发生版本跳跃，需要自动回滚事务");
    }};

    /**
     * 需要自动回滚异常
     *
     * @param subId 子错误号
     */
    public NeedRollbackException(int subId) {
        super(5003, subId, REASONS.get(subId));
    }

    /**
     * 需要自动回滚异常
     *
     * @param subId 子错误号
     */
    public NeedRollbackException(int subId, Throwable cause) {
        super(5003, subId, REASONS.get(subId), cause);
    }
}
