package net.kaaass.rumbase.page.exception;

import net.kaaass.rumbase.exception.RumbaseException;

import java.util.HashMap;
import java.util.Map;

/**
 * E9003 缓冲异常
 * <p>
 * E9003-1  内存不足无法换入
 * E9003-2  内存中所有页均被钉住无法换出
 * E9003-3  占用非空内存位置
 *
 * @author XuanLaoYee
 */
public class BufferException extends RumbaseException {
    public static final Map<Integer, String> REASONS = new HashMap<Integer, String>() {{
        put(1, "内存不足无法换入");
        put(2, "内存中所有页均被钉住无法换出");
        put(3, "占用非空内存位置");
    }};

    /**
     * 文件异常
     *
     * @param subId 子错误号
     */
    public BufferException(int subId) {
        super(9003, subId, REASONS.get(subId));
    }
}
