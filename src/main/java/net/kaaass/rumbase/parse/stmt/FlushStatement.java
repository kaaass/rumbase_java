package net.kaaass.rumbase.parse.stmt;

import net.kaaass.rumbase.parse.ISqlStatement;
import net.kaaass.rumbase.parse.ISqlStatementVisitor;

/**
 * SQL语法树：刷新缓冲区
 *
 * @author kaaass
 */
public class FlushStatement implements ISqlStatement {
    @Override
    public <T> T accept(ISqlStatementVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
