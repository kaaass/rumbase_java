package net.kaaass.rumbase.parse.parser;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.parse.ColumnIdentifier;
import net.kaaass.rumbase.parse.SqlParser;
import net.kaaass.rumbase.parse.exception.SqlSyntaxException;
import net.kaaass.rumbase.parse.stmt.SelectStatement;

import java.util.HashMap;

@Slf4j
public class SelectStatementParserTest extends TestCase {

    public void testParse() throws SqlSyntaxException {
        var sql = "SELECT distinct name, account.ID, account.balance \n" +
                "from account join payment on account.ID = payment.ID, file\n" +
                "WHERE account.ID > 1 and (payment.type = 'N' or payment.type = 'T') \n" +
                "order by account.ID desc;";
        // 解析
        var stmt = SqlParser.parseStatement(sql);
        assertTrue(stmt instanceof SelectStatement);
        log.info("Parsed: {}", stmt);
        // 测试
        var where = ((SelectStatement) stmt).getWhere();
        var params = new HashMap<ColumnIdentifier, Object>();
        params.put(new ColumnIdentifier("account", "ID"), 2);
        params.put(new ColumnIdentifier("payment", "type"), "L");
        assertFalse(where.evaluate(params));
        //
        params.put(new ColumnIdentifier("payment", "type"), "N");
        assertTrue(where.evaluate(params));
        // TODO 其他测试暂时懒得写，太麻烦了，目检正确
    }
}