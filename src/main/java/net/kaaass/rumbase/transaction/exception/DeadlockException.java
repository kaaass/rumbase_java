package net.kaaass.rumbase.transaction.exception;


import net.kaaass.rumbase.exception.RumbaseException;

import java.util.HashMap;
import java.util.Map;

/**
 * E6001 死锁异常
 * <p>
 * E6001-1 发生死锁
 * </p>
 *
 * @author criki
 */
public class DeadlockException extends RumbaseException {

    public static final Map<Integer, String> REASONS = new HashMap<>() {{
        put(1, "发生死锁");
    }};

    /**
     * 死锁异常
     *
     * @param subId 子错误号
     */
    public DeadlockException(int subId) {
        super(6001, subId, REASONS.get(subId));
    }
}
