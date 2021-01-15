package net.kaaass.rumbase.parse;

import net.kaaass.rumbase.parse.exception.SqlSyntaxException;
import net.kaaass.rumbase.parse.parser.CreateTableStatementParser;
import net.kaaass.rumbase.parse.parser.InsertStatementParser;
import net.kaaass.rumbase.parse.parser.JsqlpStatementParser;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.Statement;

import java.util.ArrayList;
import java.util.List;

/**
 * 解析任意给定的SQL语句
 *
 * @author kaaass
 */
public class SqlParser {

    private static List<JsqlpStatementParser> jsqlpStatementParsers = new ArrayList<>() {{
        add(new InsertStatementParser());
        add(new CreateTableStatementParser());
    }};

    /**
     * 将语句解析为SQL语法树
     */
    public static ISqlStatement parseStatement(String sql) throws SqlSyntaxException {
        // TODO
        // 尝试 JSqlParser 解析
        Statement stmt;
        try {
            stmt = CCJSqlParserUtil.parse(sql);
        } catch (JSQLParserException e) {
            throw new SqlSyntaxException(1, e);
        }
        for (var parser : jsqlpStatementParsers) {
            if (parser.checkStatement(stmt)) {
                return parser.parse(stmt);
            }
        }
        throw new SqlSyntaxException(2);
    }

    /**
     * 语句解析器的通用接口
     */
    public interface StatementParser<T> {

        /**
         * 将输入解析为SQL语法树
         */
        ISqlStatement parse(T input) throws SqlSyntaxException;

        /**
         * 检查语句可否被当前解析器解析
         */
        boolean checkStatement(T input);
    }
}
