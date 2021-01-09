package net.kaaass.rumbase.page.exception;

import net.kaaass.rumbase.exception.RumbaseException;

import java.util.HashMap;
import java.util.Map;

/**
 * E9002 文件异常
 * <p>
 * E9002-1  创建文件失败
 *
 * @author XuanLaoYee
 */
public class PageException extends RumbaseException {
    public static final Map<Integer, String> REASONS = new HashMap<Integer, String>(){{
        put(1, "回写数据偏移与大小之和超过规定");
    }};

    /**
     * 页操作异常
     *
     * @param subId 主错误号
     */
    public PageException(int subId){
        super(9002, subId, REASONS.get(subId));
    }
}
