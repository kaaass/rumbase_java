package net.kaaass.rumbase.parse.parser;

import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.parse.ColumnIdentifier;
import net.kaaass.rumbase.parse.ConditionExpression;
import net.kaaass.rumbase.parse.ISqlStatement;
import net.kaaass.rumbase.parse.exception.SqlSyntaxException;
import net.kaaass.rumbase.parse.stmt.SelectStatement;
import net.sf.jsqlparser.expression.ExpressionVisitorAdapter;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 将选择语句解释为对应语法树
 *
 * @author kaaass
 */
@Slf4j
public class SelectStatementParser implements JsqlpStatementParser {
    @Override
    public ISqlStatement parse(Statement input) throws SqlSyntaxException {
        final PlainSelect[] selectStmtArr = {null};
        ((Select) input).getSelectBody().accept(new SelectVisitorAdapter() {
            @Override
            public void visit(PlainSelect plainSelect) {
                selectStmtArr[0] = plainSelect;
            }
        });
        PlainSelect stmt = selectStmtArr[0];
        // 解析distinct
        var distinct = stmt.getDistinct() != null;
        // 解析表名
        if (stmt.getFromItem() == null) {
            throw new SqlSyntaxException(1, "选择的目标表不能为空");
        }
        var tableName = getTableFromItem(stmt.getFromItem());
        // 解析列
        List<ColumnIdentifier> columns = new ArrayList<>();
        var columnVisitor = new ExpressionVisitorAdapter() {

            @Override
            public void visit(Column column) {
                columns.add(ParserUtil.mapColumn(column, tableName));
            }
        };
        stmt.getSelectItems().forEach(selectItem -> selectItem.accept(new SelectItemVisitorAdapter() {

            @Override
            public void visit(AllColumns column) {
                columns.clear();
            }

            @Override
            public void visit(SelectExpressionItem item) {
                item.getExpression().accept(columnVisitor);
            }
        }));
        // 解析join
        List<SelectStatement.JoinTable> joins = null;
        if (stmt.getJoins() != null) {
            joins = stmt.getJoins().stream()
                    .map(join -> {
                        ConditionExpression joinOn = null;
                        if (join.getOnExpression() != null) {
                            joinOn = new ConditionExpression(join.getOnExpression(), tableName);
                        }
                        var table = getTableFromItem(join.getRightItem());
                        var result = new SelectStatement.JoinTable(table, joinOn);
                        result.setOuter(join.isOuter());
                        result.setRight(join.isRight());
                        result.setLeft(join.isLeft());
                        result.setNatural(join.isNatural());
                        result.setFull(join.isFull());
                        result.setInner(join.isInner());
                        result.setSimple(join.isSimple());
                        return result;
                    })
                    .collect(Collectors.toList());
        }
        // 解析where
        ConditionExpression where = null;
        if (stmt.getWhere() != null) {
            where = new ConditionExpression(stmt.getWhere(), "");
        }
        // 解析orderBy
        ColumnIdentifier[] columnIdentifiers = new ColumnIdentifier[1];
        var orderVisitor = new ExpressionVisitorAdapter() {

            @Override
            public void visit(Column column) {
                columnIdentifiers[0] = new ColumnIdentifier(column.getTable().getName(), column.getColumnName());
            }
        };
        List<SelectStatement.OrderBy> orderBys = null;
        if (stmt.getOrderByElements() != null) {
            stmt.getOrderByElements().stream()
                    .map(orderByElement -> {
                        orderByElement.getExpression().accept(orderVisitor);
                        return new SelectStatement.OrderBy(columnIdentifiers[0], orderByElement.isAsc());
                    })
                    .collect(Collectors.toList());
        }
        // 拼接结果
        return new SelectStatement(distinct, columns, tableName, joins, where, orderBys);
    }

    private String getTableFromItem(FromItem fi) {
        final String[] tableNames = new String[1];
        fi.accept(new FromItemVisitorAdapter() {

            @Override
            public void visit(Table table) {
                tableNames[0] = table.getName();
            }
        });
        return tableNames[0];
    }

    @Override
    public boolean checkStatement(Statement input) {
        return input instanceof Select;
    }
}
