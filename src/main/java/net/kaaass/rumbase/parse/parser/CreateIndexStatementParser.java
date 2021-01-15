package net.kaaass.rumbase.parse.parser;

import net.kaaass.rumbase.parse.ColumnIdentifier;
import net.kaaass.rumbase.parse.CreateIndexStatement;
import net.kaaass.rumbase.parse.ISqlStatement;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.index.CreateIndex;

import java.util.stream.Collectors;

/**
 * 将建索引语句解释为对应语法树
 *
 * @author kaaass
 */
public class CreateIndexStatementParser implements JsqlpStatementParser {
    @Override
    public ISqlStatement parse(Statement input) {
        var stmt = (CreateIndex) input;
        // 解析索引名
        var indexName = stmt.getIndex().getName();
        // 解析表名
        var tableName = stmt.getTable().getName();
        // 解析字段
        var columns = stmt.getIndex().getColumnsNames().stream()
                .map(name -> new ColumnIdentifier(tableName, name))
                .collect(Collectors.toList());
        return new CreateIndexStatement(indexName, tableName, columns);
    }

    @Override
    public boolean checkStatement(Statement input) {
        return input instanceof CreateIndex;
    }
}
