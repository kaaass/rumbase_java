package net.kaaass.rumbase.parse.parser;

import net.kaaass.rumbase.parse.ConditionExpression;
import net.kaaass.rumbase.parse.ISqlStatement;
import net.kaaass.rumbase.parse.stmt.DeleteStatement;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;

/**
 * 将删除语句解释为对应语法树
 *
 * @author kaaass
 */
public class DeleteStatementParser implements JsqlpStatementParser {
    @Override
    public ISqlStatement parse(Statement input) {
        Delete stmt = (Delete) input;
        // 解析表名
        var tableName = stmt.getTable().getName();
        // 解析where
        ConditionExpression where = null;
        if (stmt.getWhere() != null) {
            where = new ConditionExpression(stmt.getWhere(), tableName);
        }
        // 拼接结果
        return new DeleteStatement(tableName, where);
    }

    @Override
    public boolean checkStatement(Statement input) {
        return input instanceof Delete;
    }
}
