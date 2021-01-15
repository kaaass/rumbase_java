package net.kaaass.rumbase.index.exception;

import lombok.Getter;
import net.kaaass.rumbase.exception.RumbaseException;

import java.util.HashMap;
import java.util.Map;

/**
 * 要找的条目不在本页，而下一页
 * @author 无索魏
 */
public class ItemInNextPageException extends RumbaseException {

    @Getter
    private long nextPageNum;

    /**
     * 构造Rumbase异常
     *
     * @param mainId 主错误号
     * @param subId  子错误号
     * @param reason 错误原因
     */
    public ItemInNextPageException(int mainId, int subId, String reason) {
        super(mainId, subId, reason);
    }

    public static final Map<Integer, String> REASONS = new HashMap<>() {{
        put(1, "页类型不存在");
    }};

    /**
     * 条目不在本页
     *
     * @param subId 子错误号
     */
    public ItemInNextPageException(int subId, long nextPageNum) {
        super(9004, subId, REASONS.get(subId));
        this.nextPageNum = nextPageNum;
    }
}
