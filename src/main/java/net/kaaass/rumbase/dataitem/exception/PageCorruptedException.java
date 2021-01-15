package net.kaaass.rumbase.dataitem.exception;

import net.kaaass.rumbase.exception.RumbaseRuntimeException;

import java.util.HashMap;
import java.util.Map;

/**
 * 对数据库进行解析时出现的异常错误
 *
 * @author kaito
 */
public class PageCorruptedException extends RumbaseRuntimeException {
    public static final Map<Integer, String> REASONS = new HashMap<>() {{
        put(1, "没有相应表头信息或表头信息损坏");
        put(2, "数据项信息损坏");
        put(3, "数据插入异常");
    }};

    public PageCorruptedException(int subID) {
        super(7002, subID, REASONS.get(subID));
    }

    public PageCorruptedException(int subID, Throwable e) {
        super(7002, subID, REASONS.get(subID), e);
    }
}
