package net.kaaass.rumbase.parse.parser;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.parse.ColumnIdentifier;
import net.kaaass.rumbase.parse.SqlParser;
import net.kaaass.rumbase.parse.exception.SqlSyntaxException;
import net.kaaass.rumbase.parse.stmt.UpdateStatement;

import java.util.HashMap;
import java.util.List;

@Slf4j
public class UpdateStatementParserTest extends TestCase {

    public void testParse() throws SqlSyntaxException {
        var sql = "UPDATE Person SET Address = 'Zhongshan 23', City = 'Nanjing'\n" +
                "WHERE LastName = 'Wilson'";
        // 解析
        var stmt = SqlParser.parseStatement(sql);
        assertTrue(stmt instanceof UpdateStatement);
        log.info("Parsed: {}", stmt);
        // 比较
        assertEquals("Person", ((UpdateStatement) stmt).getTableName());
        assertEquals(List.of(
                new ColumnIdentifier("Person", "Address"),
                new ColumnIdentifier("Person", "City")
        ), ((UpdateStatement) stmt).getColumns());
        assertEquals("Person", ((UpdateStatement) stmt).getTableName());
        assertEquals(List.of("'Zhongshan 23'", "'Nanjing'"), ((UpdateStatement) stmt).getValues());
        // 测试 where
        var params = new HashMap<ColumnIdentifier, Object>();
        params.put(new ColumnIdentifier("Person", "LastName"), "Wilson");
        assertTrue(((UpdateStatement) stmt).getWhere().evaluate(params));
        //
        params.put(new ColumnIdentifier("Person", "LastName"), "KAAAsS");
        assertFalse(((UpdateStatement) stmt).getWhere().evaluate(params));
    }

    public void testParseNull() throws SqlSyntaxException {
        var sql = "UPDATE Person SET Address = 'Zhongshan 23', City = 'Nanjing'";
        // 解析
        var stmt = SqlParser.parseStatement(sql);
        assertTrue(stmt instanceof UpdateStatement);
        log.info("Parsed: {}", stmt);
        // 比较
        assertEquals("Person", ((UpdateStatement) stmt).getTableName());
        assertEquals(List.of(
                new ColumnIdentifier("Person", "Address"),
                new ColumnIdentifier("Person", "City")
        ), ((UpdateStatement) stmt).getColumns());
        assertEquals("Person", ((UpdateStatement) stmt).getTableName());
        assertEquals(List.of("'Zhongshan 23'", "'Nanjing'"), ((UpdateStatement) stmt).getValues());
        assertNull(((UpdateStatement) stmt).getWhere());
    }
}