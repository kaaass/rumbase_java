package net.kaaass.rumbase.page.exception;

import net.kaaass.rumbase.exception.RumbaseException;

import java.util.HashMap;
import java.util.Map;

/**
 * E9003 缓冲异常
 * <p>
 * E9003-1  内存不足无法换入
 *
 * @author XuanLaoYee
 */
public class BufferExeception extends RumbaseException {
    public static final Map<Integer, String> REASONS = new HashMap<Integer, String>() {{
        put(1, "内存不足无法换入");
    }};

    /**
     * 文件异常
     *
     * @param subId 子错误号
     */
    public BufferExeception(int subId) {
        super(9003, subId, REASONS.get(subId));
    }
}
