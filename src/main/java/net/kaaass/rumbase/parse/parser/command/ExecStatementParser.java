package net.kaaass.rumbase.parse.parser.command;

import net.kaaass.rumbase.parse.ISqlStatement;
import net.kaaass.rumbase.parse.SqlParser;
import net.kaaass.rumbase.parse.stmt.ExecStatement;

/**
 * 将执行SQL文件语句解释为对应语法树
 *
 * @author kaaass
 */
public class ExecStatementParser implements SqlParser.StatementParser<String> {
    @Override
    public ISqlStatement parse(String input) {
        return new ExecStatement(input.substring(5).trim());
    }

    @Override
    public boolean checkStatement(String input) {
        return input.startsWith("exec ") || input.startsWith("EXEC ");
    }
}
