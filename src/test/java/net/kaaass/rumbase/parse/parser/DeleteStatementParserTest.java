package net.kaaass.rumbase.parse.parser;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.parse.ColumnIdentifier;
import net.kaaass.rumbase.parse.SqlParser;
import net.kaaass.rumbase.parse.exception.SqlSyntaxException;
import net.kaaass.rumbase.parse.stmt.DeleteStatement;

import java.util.HashMap;

@Slf4j
public class DeleteStatementParserTest extends TestCase {

    public void testParse() throws SqlSyntaxException {
        var sql = "DELETE FROM Person WHERE LastName = 'Wilson'";
        // 解析
        var stmt = SqlParser.parseStatement(sql);
        assertTrue(stmt instanceof DeleteStatement);
        log.info("Parsed: {}", stmt);
        // 比较
        assertEquals("Person", ((DeleteStatement) stmt).getTableName());
        // 测试 where
        var params = new HashMap<ColumnIdentifier, Object>();
        params.put(new ColumnIdentifier("Person", "LastName"), "Wilson");
        assertTrue(((DeleteStatement) stmt).getWhere().evaluate(params));
        //
        params.put(new ColumnIdentifier("Person", "LastName"), "KAAAsS");
        assertFalse(((DeleteStatement) stmt).getWhere().evaluate(params));
    }

    public void testParseNull() throws SqlSyntaxException {
        var sql = "DELETE FROM Person ";
        // 解析
        var stmt = SqlParser.parseStatement(sql);
        assertTrue(stmt instanceof DeleteStatement);
        log.info("Parsed: {}", stmt);
        // 比较
        assertEquals("Person", ((DeleteStatement) stmt).getTableName());
        assertNull(((DeleteStatement) stmt).getWhere());
    }
}