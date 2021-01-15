package net.kaaass.rumbase.parse.parser;

import net.kaaass.rumbase.parse.SqlParser;
import net.sf.jsqlparser.statement.Statement;

/**
 * 适配JSqlParser库的解析器
 *
 * @author kaaass
 */
public interface JsqlpStatementParser extends SqlParser.StatementParser<Statement> {
}
