package net.kaaass.rumbase.parse.parser;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.parse.ColumnIdentifier;
import net.kaaass.rumbase.parse.CreateIndexStatement;
import net.kaaass.rumbase.parse.SqlParser;
import net.kaaass.rumbase.parse.exception.SqlSyntaxException;

import java.util.ArrayList;

@Slf4j
public class CreateIndexStatementParserTest extends TestCase {

    public void testParseSingle() throws SqlSyntaxException {
        var sql = "CREATE INDEX PersonIndex ON Person (LastName) ;";
        // 解析
        var stmt = SqlParser.parseStatement(sql);
        assertTrue(stmt instanceof CreateIndexStatement);
        log.info("Parsed: {}", stmt);
        // 准备预期结果
        var columns = new ArrayList<ColumnIdentifier>();
        columns.add(new ColumnIdentifier("Person", "LastName"));
        var expected = new CreateIndexStatement(
                "PersonIndex",
                "Person",
                columns
        );
        // 比较
        assertEquals(expected, stmt);
    }

    public void testParseMulti() throws SqlSyntaxException {
        var sql = "CREATE INDEX PersonIndex ON Person (LastName, ID) ;";
        // 解析
        var stmt = SqlParser.parseStatement(sql);
        assertTrue(stmt instanceof CreateIndexStatement);
        log.info("Parsed: {}", stmt);
        // 准备预期结果
        var columns = new ArrayList<ColumnIdentifier>();
        columns.add(new ColumnIdentifier("Person", "LastName"));
        columns.add(new ColumnIdentifier("Person", "ID"));
        var expected = new CreateIndexStatement(
                "PersonIndex",
                "Person",
                columns
        );
        // 比较
        assertEquals(expected, stmt);
    }
}