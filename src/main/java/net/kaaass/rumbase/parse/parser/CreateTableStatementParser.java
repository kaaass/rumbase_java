package net.kaaass.rumbase.parse.parser;

import net.kaaass.rumbase.parse.stmt.CreateTableStatement;
import net.kaaass.rumbase.parse.ISqlStatement;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.create.table.ColDataType;
import net.sf.jsqlparser.statement.create.table.CreateTable;

import java.util.List;
import java.util.stream.Collectors;

/**
 * 将建表语句解释为对应语法树
 *
 * @author kaaass
 */
public class CreateTableStatementParser implements JsqlpStatementParser {
    @Override
    public ISqlStatement parse(Statement input) {
        var stmt = (CreateTable) input;
        // 解析表名
        var tableName = stmt.getTable().getName();
        // 解析字段定义
        var columnDefs = stmt.getColumnDefinitions().stream()
                .map(def -> new CreateTableStatement.ColumnDefinition(
                        mapColType(def.getColDataType()),
                        def.getColumnName(),
                        def.getColumnSpecs() != null
                                && checkNotNull(def.getColumnSpecs())
                ))
                .collect(Collectors.toList());
        return new CreateTableStatement(tableName, columnDefs);
    }

    private boolean checkNotNull(List<String> specs) {
        var a = specs.indexOf("not");
        if (a < 0) {
            a = specs.indexOf("NOT");
        }
        var b = specs.indexOf("null");
        if (b < 0) {
            b = specs.indexOf("NULL");
        }
        return a >= 0 && b > a;
    }

    public static CreateTableStatement.ColumnType mapColType(ColDataType type) {
        return new CreateTableStatement.ColumnType(type.getDataType(), type.getArgumentsStringList());
    }

    @Override
    public boolean checkStatement(Statement input) {
        return input instanceof CreateTable;
    }
}
