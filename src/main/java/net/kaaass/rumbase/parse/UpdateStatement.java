package net.kaaass.rumbase.parse;

import lombok.Data;

import java.util.List;

/**
 * SQL语法树：更新语句
 * @author kaaass
 */
@Data
public class UpdateStatement {

    /**
     * 更新的目标表名
     */
    private String tableName;

    /**
     * 要更新的列，与值对应
     */
    private List<ColumnIdentifier> columns;

    /**
     * 更新的目标值
     */
    private List<String> values;

    /**
     * 更新行的条件
     */
    private ConditionExpression where;
}
