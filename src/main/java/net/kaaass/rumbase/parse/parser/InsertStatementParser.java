package net.kaaass.rumbase.parse.parser;

import net.kaaass.rumbase.parse.ColumnIdentifier;
import net.kaaass.rumbase.parse.ISqlStatement;
import net.kaaass.rumbase.parse.stmt.InsertStatement;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.ItemsListVisitorAdapter;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.insert.Insert;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * 将插入语句解释为对应语法树
 * @author kaaass
 */
public class InsertStatementParser implements JsqlpStatementParser {
    @Override
    public ISqlStatement parse(Statement input) {
        var stmt = (Insert) input;
        // 解析表名
        var tableName = stmt.getTable().getName();
        // 解析列
        List<ColumnIdentifier> columns = null;
        var parsedColumn = stmt.getColumns();
        if (parsedColumn != null) {
            columns = ParserUtil.mapColumnList(parsedColumn, tableName);
        }
        // 解析插入的数据
        var values = new ArrayList<String>();
        stmt.getItemsList().accept(new ItemsListVisitorAdapter(){

            @Override
            public void visit(ExpressionList expressionList) {
                expressionList.getExpressions().stream()
                        .map(Objects::toString)
                        .forEach(values::add);
            }
        });
        // 拼接结果
        return new InsertStatement(tableName, columns, values);
    }

    @Override
    public boolean checkStatement(Statement stmt) {
        return stmt instanceof Insert;
    }
}
