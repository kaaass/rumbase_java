package net.kaaass.rumbase.parse.parser;

import net.kaaass.rumbase.parse.ColumnIdentifier;
import net.kaaass.rumbase.parse.ConditionExpression;
import net.kaaass.rumbase.parse.ISqlStatement;
import net.kaaass.rumbase.parse.exception.SqlSyntaxException;
import net.kaaass.rumbase.parse.stmt.UpdateStatement;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.update.Update;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * 将更新语句解释为对应语法树
 *
 * @author kaaass
 */
public class UpdateStatementParser implements JsqlpStatementParser {
    @Override
    public ISqlStatement parse(Statement input) {
        Update stmt = (Update) input;
        // 解析表名
        var tableName = stmt.getTable().getName();
        // 解析列
        List<ColumnIdentifier> columns = null;
        var parsedColumn = stmt.getColumns();
        if (parsedColumn != null) {
            columns = ParserUtil.mapColumnList(parsedColumn, tableName);
        }
        // 解析插入的数据
        var values = stmt.getExpressions().stream()
                .map(Objects::toString)
                .collect(Collectors.toList());
        // 解析where
        ConditionExpression where = null;
        if (stmt.getWhere() != null) {
            where = new ConditionExpression(stmt.getWhere(), tableName);
        }
        return new UpdateStatement(tableName, columns, values, where);
    }

    @Override
    public boolean checkStatement(Statement input) {
        return input instanceof Update;
    }
}
