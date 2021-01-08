package net.kaaass.rumbase.table.exception;

import net.kaaass.rumbase.exception.RumbaseException;

import java.util.HashMap;
import java.util.Map;

/**
 * E3001 关系不存在异常
 * <p>
 * E3001-1 关系不存在
 * <p>
 * E3001-2 字段不存在
 * <p>
 * E3001-3 视图不存在
 * <p>
 * E3001-4 元组不存在
 * <p>
 *
 *
 * @author @KveinAxel
 */
public class TableNotFoundException extends RumbaseException {
    public static final Map<Integer, String> REASONS = new HashMap<>(){{
        put(1, "关系不存在");
        put(2, "字段不存在");
        put(3, "视图不存在");
        put(4, "元组不存在");

    }};

    /**
     * 关系不存在异常
     *
     * @param subId  子错误号
     */
    public TableNotFoundException(int subId) {
        super(3001, subId, REASONS.get(subId));
    }
}
