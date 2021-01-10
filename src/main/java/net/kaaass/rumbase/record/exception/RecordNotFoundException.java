package net.kaaass.rumbase.record.exception;

import net.kaaass.rumbase.exception.RumbaseException;

import java.util.HashMap;
import java.util.Map;

/**
 * E5001 记录不存在异常
 * <p>
 * E5001-1 物理记录不存在
 *
 * @author kaaass
 */
public class RecordNotFoundException extends RumbaseException {

    public static final Map<Integer, String> REASONS = new HashMap<>() {{
        put(1, "物理记录不存在");
    }};

    /**
     * 记录不存在异常
     *
     * @param subId 子错误号
     */
    public RecordNotFoundException(int subId) {
        super(5001, subId, REASONS.get(subId));
    }
}
