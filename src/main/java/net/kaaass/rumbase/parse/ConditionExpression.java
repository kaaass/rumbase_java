package net.kaaass.rumbase.parse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * 条件表达式操作相关
 *
 * @author kaaass
 */
public class ConditionExpression {

    /**
     * 根据参数列表求值
     */
    public boolean evaluate(Map<ColumnIdentifier, Object> paramMap) {
        // TODO
        return true;
    }

    /**
     * 获得表达式求值需要的参数
     */
    public List<ColumnIdentifier> getParams() {
        // TODO
        return new ArrayList<>();
    }
}
