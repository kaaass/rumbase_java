package net.kaaass.rumbase.parse.stmt;

import lombok.Data;
import lombok.RequiredArgsConstructor;
import net.kaaass.rumbase.parse.ISqlStatement;
import net.kaaass.rumbase.parse.ISqlStatementVisitor;

/**
 * SQL语法树：执行SQL文件
 *
 * @author kaaass
 */
@Data
@RequiredArgsConstructor
public class ExecStatement implements ISqlStatement {

    private final String filepath;

    @Override
    public <T> T accept(ISqlStatementVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
