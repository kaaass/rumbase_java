package net.kaaass.rumbase.transaction.exception;


import net.kaaass.rumbase.exception.RumbaseException;

import java.util.HashMap;
import java.util.Map;

/**
 * E6002 事务状态异常
 * <p>
 * E6002-1 事务状态异常
 *
 * @author criki
 */
public class StatusException extends RumbaseException {

    public static final Map<Integer, String> REASONS = new HashMap<>() {{
        put(1, "事务状态异常");
    }};

    /**
     * 事务状态异常
     *
     * @param subId 子错误号
     */
    public StatusException(int subId) {
        super(6001, subId, REASONS.get(subId));
    }

}
