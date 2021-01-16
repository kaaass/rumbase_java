package net.kaaass.rumbase.parse;

import net.kaaass.rumbase.parse.stmt.*;

/**
 * SQL语句访问者，用于处理对应SQL语句
 *
 * @author kaaass
 */
public interface ISqlStatementVisitor<T> {

    T visit(SelectStatement statement);

    T visit(InsertStatement statement);

    T visit(UpdateStatement statement);

    T visit(DeleteStatement statement);

    T visit(CreateIndexStatement statement);

    T visit(CreateTableStatement statement);
}
