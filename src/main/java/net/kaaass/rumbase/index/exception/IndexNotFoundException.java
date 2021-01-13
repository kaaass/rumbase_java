package net.kaaass.rumbase.index.exception;

import net.kaaass.rumbase.exception.RumbaseException;

import java.util.HashMap;
import java.util.Map;

/**
 * 当加载一个索引时，如果索引实现不存在相应的持久化信息，则无法加载，并抛出此异常
 * @author 无索魏
 */
public class IndexNotFoundException extends RumbaseException {
    /**
     * 构造Rumbase异常
     *
     * @param mainId 主错误号
     * @param subId  子错误号
     * @param reason 错误原因
     */
    public IndexNotFoundException(int mainId, int subId, String reason) {
        super(mainId, subId, reason);
    }

    public static final Map<Integer, String> REASONS = new HashMap<>() {{
        put(1, "索引不存在");
    }};

    /**
     * 索引不存在
     *
     * @param subId 子错误号
     */
    public IndexNotFoundException(int subId) {
        super(9001, subId, REASONS.get(subId));
    }
}
