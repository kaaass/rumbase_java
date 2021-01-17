package net.kaaass.rumbase.parse.stmt;

import lombok.AllArgsConstructor;
import lombok.Data;
import net.kaaass.rumbase.parse.ConditionExpression;
import net.kaaass.rumbase.parse.ISqlStatement;
import net.kaaass.rumbase.parse.ISqlStatementVisitor;

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

    @Override
    public <T> T accept(ISqlStatementVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
