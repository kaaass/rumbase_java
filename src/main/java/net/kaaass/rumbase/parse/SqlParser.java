package net.kaaass.rumbase.parse;

import net.kaaass.rumbase.parse.exception.SqlSyntaxException;
import net.kaaass.rumbase.parse.parser.CommandStatementParser;
import net.kaaass.rumbase.parse.parser.JsqlpStatementParser;
import net.kaaass.rumbase.parse.parser.command.ExecStatementParser;
import net.kaaass.rumbase.parse.parser.jsqlp.*;
import net.kaaass.rumbase.parse.stmt.*;
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

    private static final List<StatementParser<String>> STRING_STATEMENT_PARSERS = new ArrayList<>() {{
        add(new CommandStatementParser(StartTransactionStatement.class, "start transaction"));
        add(new CommandStatementParser(CommitStatement.class, "commit"));
        add(new CommandStatementParser(RollbackStatement.class, "rollback"));
        add(new CommandStatementParser(ExitStatement.class, "exit"));
        add(new CommandStatementParser(ShutdownStatement.class, "shutdown"));
        add(new CommandStatementParser(FlushStatement.class, "flush"));
        add(new ExecStatementParser());
    }};

    private static final List<JsqlpStatementParser> JSQLP_STATEMENT_PARSERS = new ArrayList<>() {{
        add(new SelectStatementParser());
        add(new InsertStatementParser());
        add(new UpdateStatementParser());
        add(new DeleteStatementParser());
        add(new CreateTableStatementParser());
        add(new CreateIndexStatementParser());
    }};

    /**
     * 将语句解析为SQL语法树
     */
    public static ISqlStatement parseStatement(String sql) throws SqlSyntaxException {
        // 尝试字符串解析器解析
        for (var parser : STRING_STATEMENT_PARSERS) {
            if (parser.checkStatement(sql)) {
                return parser.parse(sql);
            }
        }
        // 尝试 JSqlParser 解析
        Statement stmt;
        try {
            stmt = CCJSqlParserUtil.parse(sql);
        } catch (JSQLParserException e) {
            throw new SqlSyntaxException(1, e);
        }
        for (var parser : JSQLP_STATEMENT_PARSERS) {
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
