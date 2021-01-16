package net.kaaass.rumbase.parse;

import net.kaaass.rumbase.parse.stmt.*;

/**
 * SQL语句访问者，用于处理对应SQL语句
 *
 * @author kaaass
 */
public interface ISqlStatementVisitor {

    void visit(SelectStatement statement);

    void visit(InsertStatement statement);

    void visit(UpdateStatement statement);

    void visit(DeleteStatement statement);

    void visit(CreateIndexStatement statement);

    void visit(CreateTableStatement statement);
}
