package net.kaaass.rumbase.parse.exception;

import net.kaaass.rumbase.exception.RumbaseException;

import java.util.HashMap;
import java.util.Map;

/**
 * E1001 记录不存在异常
 * <p>
 * E1001-1 SQL语句语法错误
 * E1001-2 不支持的SQL语句
 *
 * @author kaaass
 */
public class SqlSyntaxException extends RumbaseException {

    public static final Map<Integer, String> REASONS = new HashMap<>() {{
        put(1, "SQL语句语法错误");
        put(2, "不支持的SQL语句");
    }};

    public SqlSyntaxException(int subId) {
        super(5001, subId, REASONS.get(subId));
    }

    public SqlSyntaxException(int subId, String subReason) {
        super(5001, subId, REASONS.get(subId) + "：" + subReason);
    }

    public SqlSyntaxException(int subId, Throwable e) {
        super(5001, subId, REASONS.get(subId), e);
    }
}
