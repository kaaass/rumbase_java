package net.kaaass.rumbase.parse.stmt;

import lombok.AllArgsConstructor;
import lombok.Data;
import net.kaaass.rumbase.parse.ColumnIdentifier;
import net.kaaass.rumbase.parse.ConditionExpression;
import net.kaaass.rumbase.parse.ISqlStatement;
import net.kaaass.rumbase.parse.ISqlStatementVisitor;

import java.util.List;

/**
 * SQL语法树：更新语句
 *
 * @author kaaass
 */
@Data
@AllArgsConstructor
public class UpdateStatement implements ISqlStatement {

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

    @Override
    public void accept(ISqlStatementVisitor visitor) {
        visitor.visit(this);
    }
}
