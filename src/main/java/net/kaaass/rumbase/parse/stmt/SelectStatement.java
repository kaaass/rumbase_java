package net.kaaass.rumbase.parse.stmt;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.RequiredArgsConstructor;
import net.kaaass.rumbase.parse.ColumnIdentifier;
import net.kaaass.rumbase.parse.ConditionExpression;
import net.kaaass.rumbase.parse.ISqlStatement;
import net.kaaass.rumbase.parse.ISqlStatementVisitor;

import java.util.List;

/**
 * SQL语法树：选择语句
 *
 * @author kaaass
 */
@Data
@AllArgsConstructor
public class SelectStatement implements ISqlStatement {

    /**
     * 选择的结果行是否不允许重复
     */
    private boolean distinct;

    /**
     * 选择的字段，如果为null则代表选择全部字段，即“*”
     */
    private List<ColumnIdentifier> selectColumns;

    /**
     * 选择的源表，其他表以Join的方式表示
     */
    private String fromTable;

    /**
     * 需要Join的表，按顺序从fromTable开始连接，为null代表不join
     * 如：a join b on cond1 join c on cond2, d
     * 则fromTable为a，joins有3项[b, c, d]
     */
    private List<JoinTable> joins;

    /**
     * 选择时用于过滤的条件，为null代表不进行过滤
     */
    private ConditionExpression where;

    /**
     * 选择后结果排序的规则，为null代表不排序
     */
    private List<OrderBy> orderBys;

    @Override
    public <T> T accept(ISqlStatementVisitor<T> visitor) {
        return visitor.visit(this);
    }

    /**
     * SQL语法树：Join表
     */
    @Data
    @RequiredArgsConstructor
    public static class JoinTable {

        private final String tableName;

        private final ConditionExpression joinOn;

        /*
         * Join方式
         */

        private boolean outer = false;
        private boolean right = false;
        private boolean left = false;
        private boolean natural = false;
        private boolean full = false;
        private boolean inner = false;
        private boolean simple = false;
    }

    /**
     * SQL语法树：排序方式
     */
    @Data
    @AllArgsConstructor
    public static class OrderBy {

        /**
         * 排序依据的列
         */
        private ColumnIdentifier column;

        /**
         * 是否升序
         */
        private boolean ascending;
    }
}
