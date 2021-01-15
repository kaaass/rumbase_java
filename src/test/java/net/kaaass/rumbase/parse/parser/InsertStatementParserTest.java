package net.kaaass.rumbase.parse.parser;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.parse.ColumnIdentifier;
import net.kaaass.rumbase.parse.SqlParser;
import net.kaaass.rumbase.parse.exception.SqlSyntaxException;
import net.kaaass.rumbase.parse.stmt.InsertStatement;

import java.util.ArrayList;

@Slf4j
public class InsertStatementParserTest extends TestCase {

    public void testParseColumnValue() throws SqlSyntaxException {
        var sql = "INSERT INTO Persons (Persons.LastName, Address) VALUES ('Wilson', 'Champs-Elysees')";
        // 解析
        var stmt = SqlParser.parseStatement(sql);
        assertTrue(stmt instanceof InsertStatement);
        log.info("Parsed: {}", stmt);
        // 准备预期结果
        var columns = new ArrayList<ColumnIdentifier>();
        columns.add(new ColumnIdentifier("Persons", "LastName"));
        columns.add(new ColumnIdentifier("Persons", "Address"));
        var values = new ArrayList<String>();
        values.add("'Wilson'");
        values.add("'Champs-Elysees'");
        var expected = new InsertStatement(
                "Persons",
                columns,
                values
        );
        // 比较
        assertEquals(expected, stmt);
    }

    public void testParseValue() throws SqlSyntaxException {
        var sql = "INSERT INTO stu VALUES (20200101, 'KAAAsS', true, 3.9)";
        // 解析
        var stmt = SqlParser.parseStatement(sql);
        assertTrue(stmt instanceof InsertStatement);
        log.info("Parsed: {}", stmt);
        // 准备预期结果
        var values = new ArrayList<String>();
        values.add("20200101");
        values.add("'KAAAsS'");
        values.add("true");
        values.add("3.9");
        var expected = new InsertStatement(
                "stu",
                null,
                values
        );
        // 比较
        assertEquals(expected, stmt);
    }
}