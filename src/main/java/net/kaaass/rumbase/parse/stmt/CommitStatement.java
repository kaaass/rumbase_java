package net.kaaass.rumbase.parse.stmt;

import net.kaaass.rumbase.parse.ISqlStatement;
import net.kaaass.rumbase.parse.ISqlStatementVisitor;

/**
 * SQL语法树：提交事务语句
 *
 * @author kaaass
 */
public class CommitStatement implements ISqlStatement {
    @Override
    public <T> T accept(ISqlStatementVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
