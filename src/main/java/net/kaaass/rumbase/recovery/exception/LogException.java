package net.kaaass.rumbase.recovery.exception;

import net.kaaass.rumbase.exception.RumbaseException;

import java.util.HashMap;
import java.util.Map;

public class LogException extends RumbaseException {
    /**
     * 构造Rumbase异常
     *
     */

    public static final Map<Integer, String> REASONS = new HashMap<Integer, String>() {{
        put(1, "日志文件无法解析");
        put(2,"事务ID不存在");
    }};

    public LogException(int subId) {
        super(9001,subId,REASONS.get(subId));
    }
}
