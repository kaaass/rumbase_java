package net.kaaass.rumbase.parse.stmt;

import net.kaaass.rumbase.parse.ISqlStatement;
import net.kaaass.rumbase.parse.ISqlStatementVisitor;

/**
 * SQL语法树：退出会话语句
 *
 * @author kaaass
 */
public class ExitStatement implements ISqlStatement {
    @Override
    public <T> T accept(ISqlStatementVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
