package net.kaaass.rumbase.record.exception;

import net.kaaass.rumbase.exception.RumbaseException;

import java.util.HashMap;
import java.util.Map;

/**
 * E5002 存储数据错误异常
 * <p>
 * E5002-1 存储数据元信息不存在
 *
 * @author kaaass
 */
public class StorageCorruptedException extends RumbaseException {

    public static final Map<Integer, String> REASONS = new HashMap<>() {{
        put(1, "存储数据元信息不存在");
    }};

    /**
     * 记录不存在异常
     *
     * @param subId 子错误号
     */
    public StorageCorruptedException(int subId) {
        super(5002, subId, REASONS.get(subId));
    }

    public StorageCorruptedException(int subId, Throwable cause) {
        super(5002, subId, REASONS.get(subId), cause);
    }
}
