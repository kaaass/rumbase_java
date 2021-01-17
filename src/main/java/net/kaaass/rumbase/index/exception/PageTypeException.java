package net.kaaass.rumbase.index.exception;

import net.kaaass.rumbase.exception.RumbaseException;

import java.util.HashMap;
import java.util.Map;

/**
 * @author 无索魏
 */
public class PageTypeException extends RumbaseException {
    /**
     * 构造Rumbase异常
     *
     * @param mainId 主错误号
     * @param subId  子错误号
     * @param reason 错误原因
     */
    public PageTypeException(int mainId, int subId, String reason) {
        super(mainId, subId, reason);
    }

    public static final Map<Integer, String> REASONS = new HashMap<>() {{
        put(1, "页类型不存在");
        put(2, "非MEAT页不能读取文件总页数");
    }};

    /**
     * 索引不存在
     *
     * @param subId 子错误号
     */
    public PageTypeException(int subId) {
        super(9003, subId, REASONS.get(subId));
    }
}
