package net.kaaass.rumbase.dataitem.exception;

import net.kaaass.rumbase.exception.RumbaseException;

import java.util.HashMap;
import java.util.Map;

/**
 * 对数据库进行解析时出现的异常错误
 *
 * @author kaito
 */
public class ItemException extends RumbaseException {
    public static final Map<Integer, String> REASONS = new HashMap<>() {{
        put(1, "没有相应表头信息");
    }};

    public ItemException(int subID) {
        super(6001, subID, REASONS.get(subID));
    }
}
