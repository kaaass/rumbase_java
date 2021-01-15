package net.kaaass.rumbase.index.exception;

import net.kaaass.rumbase.exception.RumbaseException;

import java.util.HashMap;
import java.util.Map;

/**
 * 页已经满了，无法进行原先的插入操作
 * @author 无索魏
 */
public class PageFullException extends RumbaseException {
    /**
     * 构造Rumbase异常
     *
     * @param mainId 主错误号
     * @param subId  子错误号
     * @param reason 错误原因
     */
    public PageFullException(int mainId, int subId, String reason) {
        super(mainId, subId, reason);
    }

    public static final Map<Integer, String> REASONS = new HashMap<>() {{
        put(1, "页已满，无法插入");
    }};

    /**
     * 索引不存在
     *
     * @param subId 子错误号
     */
    public PageFullException(int subId) {
        super(9005, subId, REASONS.get(subId));
    }
}
