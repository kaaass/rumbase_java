package net.kaaass.rumbase.parse;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectVisitorAdapter;
import net.sf.jsqlparser.statement.update.Update;

@Slf4j
public class SqlTest extends TestCase {

    public void testParseSelect() throws JSQLParserException {
        Select select = (Select) CCJSqlParserUtil.parse(
                "SELECT name, a.ID as aid, `account`.`balance` " +
                        "from `account` as a join `payment` as p on a.ID = p.ID, `file` as f " +
                        "WHERE a.`ID` > 1 and (p.`type` = 'N' or p.`type` = 'T') " +
                        "order by acc.ID desc;"
        );
        final PlainSelect[] selectStmtArr = {null};
        select.getSelectBody().accept(new SelectVisitorAdapter() {
            @Override
            public void visit(PlainSelect plainSelect) {
                selectStmtArr[0] = plainSelect;
            }
        });
        PlainSelect selectStmt = selectStmtArr[0];
        log.info("Select: {}", selectStmt.getSelectItems());
        log.info("From: {}", selectStmt.getFromItem());
        log.info("Join: {}", selectStmt.getJoins());
        log.info("Where: {}", selectStmt.getWhere());
        log.info("OrderBy: {}", selectStmt.getOrderByElements());
        var expr = selectStmt.getWhere();
    }

    public void testParseInsert() throws JSQLParserException {
        Insert insert = (Insert) CCJSqlParserUtil.parse(
                "INSERT INTO Persons (LastName, Address) VALUES ('Wilson', 'Champs-Elysees')"
        );
        log.info("Table: {}", insert.getTable());
        log.info("Values: {}", insert.getItemsList());
        log.info("Columns: {}", insert.getColumns());
    }

    public void testParseUpdate() throws JSQLParserException {
        Update stmt = (Update) CCJSqlParserUtil.parse(
                "UPDATE Person SET Address = 'Zhongshan 23', City = 'Nanjing'\n" +
                        "WHERE LastName = 'Wilson'"
        );
        log.info("Table: {}", stmt.getTable());
        log.info("Columns: {}", stmt.getColumns());
        log.info("Expressions: {}", stmt.getExpressions());
        log.info("Where: {}", stmt.getWhere());
    }
}
