package net.kaaass.rumbase.parse.parser;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.parse.ISqlStatement;
import net.kaaass.rumbase.parse.SqlParser;
import net.kaaass.rumbase.parse.exception.SqlSyntaxException;

import java.lang.reflect.InvocationTargetException;

/**
 * 字符串指令解析器
 *
 * @author kaaass
 */
@Slf4j
@RequiredArgsConstructor
public class CommandStatementParser implements SqlParser.StatementParser<String> {

    /**
     * 语句解析结果的类
     */
    private final Class<? extends ISqlStatement> stmtClazz;

    /**
     * 指令格式
     */
    private final String command;

    @Override
    public ISqlStatement parse(String input) throws SqlSyntaxException {
        try {
            return stmtClazz.getDeclaredConstructor().newInstance();
        } catch (InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
            log.error("无法生成指令对象 {}", input, e);
            throw new SqlSyntaxException(3, e);
        }
    }

    @Override
    public boolean checkStatement(String input) {
        // 忽略逗号
        if (input.charAt(input.length() - 1) == ';') {
            input = input.substring(0, input.length() - 1);
        }
        return command.compareToIgnoreCase(input) == 0;
    }
}
