package net.kaaass.rumbase.index.exception;

import net.kaaass.rumbase.exception.RumbaseException;

import java.util.HashMap;
import java.util.Map;

/**
 * TODO 文档
 */
public class IndexAlreadyExistException extends RumbaseException {

    public static final Map<Integer, String> REASONS = new HashMap<>() {{
        put(1, "索引已经存在");
    }};

    /**
     * 构造Rumbase异常
     *
     * @param mainId 主错误号
     * @param subId  子错误号
     * @param reason 错误原因
     */
    private IndexAlreadyExistException(int mainId, int subId, String reason) {
        super(mainId, subId, reason);
    }

    /**
     * 索引不存在
     *
     * @param subId 子错误号
     */
    public IndexAlreadyExistException(int subId) {
        super(9002, subId, REASONS.get(subId));
    }
}
