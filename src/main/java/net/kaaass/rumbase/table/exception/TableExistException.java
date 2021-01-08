package net.kaaass.rumbase.table.exception;

import net.kaaass.rumbase.exception.RumbaseException;

import java.util.HashMap;
import java.util.Map;

/**
 * E3003 表-已存在异常
 * <p>
 * E3003-1 关系已存在
 *
 * @author @KveinAxel
 */
public class TableExistException extends RumbaseException {
    public static final Map<Integer, String> REASONS = new HashMap<>(){{
        put(1, "关系已存在");
    }};

    /**
     * 类型不匹配异常
     *
     * @param subId  子错误号
     */
    public TableExistException(int subId) {
        super(3003, subId, REASONS.get(subId));
    }
}
