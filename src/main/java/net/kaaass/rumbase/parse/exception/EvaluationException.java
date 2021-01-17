package net.kaaass.rumbase.parse.exception;

import net.kaaass.rumbase.exception.RumbaseRuntimeException;

import java.util.HashMap;
import java.util.Map;

/**
 * E1002 表达式运算错误
 * <p>
 * E1002-1 运算结果类型错误
 * E1002-2 运算过程错误，可能存在不支持的表达式
 *
 * @author kaaass
 */
public class EvaluationException extends RumbaseRuntimeException {

    public static final Map<Integer, String> REASONS = new HashMap<>() {{
        put(1, "运算结果类型错误");
        put(2, "运算过程错误，可能存在不支持的表达式");
    }};

    public EvaluationException(int subId) {
        super(5001, subId, REASONS.get(subId));
    }

    public EvaluationException(int subId, Throwable e) {
        super(5001, subId, REASONS.get(subId), e);
    }
}
