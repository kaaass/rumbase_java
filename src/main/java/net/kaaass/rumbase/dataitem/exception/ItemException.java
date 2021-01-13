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
        put(1, "要创建的文件已存在");
        put(2, "查找的文件不存在");
    }};

    public ItemException(int subID) {
        super(6001, subID, REASONS.get(subID));
    }
}
