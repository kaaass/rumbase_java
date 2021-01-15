package net.kaaass.rumbase.parse;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * SQL语法树：删除语句
 *
 * @author kaaass
 */
@Data
@AllArgsConstructor
public class DeleteStatement implements ISqlStatement {

    /**
     * 删除的目标表名
     */
    private String tableName;

    /**
     * 删除的筛选条件，为null则代表清空表
     */
    private ConditionExpression where;
}
