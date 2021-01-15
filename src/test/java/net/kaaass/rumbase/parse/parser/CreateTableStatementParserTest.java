package net.kaaass.rumbase.parse.parser;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.parse.CreateTableStatement;
import net.kaaass.rumbase.parse.SqlParser;
import net.kaaass.rumbase.parse.exception.SqlSyntaxException;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class CreateTableStatementParserTest extends TestCase {

    public void testParse() throws SqlSyntaxException {
        var sql = "CREATE TABLE Persons\n" +
                "(\n" +
                "Id_P int,\n" +
                "LastName varchar(255),\n" +
                "FirstName varchar(255)\n" +
                ")";
        // 解析
        var stmt = SqlParser.parseStatement(sql);
        assertTrue(stmt instanceof CreateTableStatement);
        log.info("Parsed: {}", stmt);
        // 准备预期结果
        var columns = new ArrayList<CreateTableStatement.ColumnDefinition>();
        columns.add(new CreateTableStatement.ColumnDefinition(
                new CreateTableStatement.ColumnType("int", null),
                "Id_P"
        ));
        columns.add(new CreateTableStatement.ColumnDefinition(
                new CreateTableStatement.ColumnType("varchar", List.of("255")),
                "LastName"
        ));
        columns.add(new CreateTableStatement.ColumnDefinition(
                new CreateTableStatement.ColumnType("varchar", List.of("255")),
                "FirstName"
        ));
        var expected = new CreateTableStatement(
                "Persons",
                columns
        );
        // 比较
        assertEquals(expected, stmt);
    }
}