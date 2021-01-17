package net.kaaass.rumbase.parse;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.parser.CCJSqlParserUtil;

import java.util.ArrayList;
import java.util.HashMap;

@Slf4j
public class ConditionExpressionTest extends TestCase {

    public void testEvaluate() throws JSQLParserException {
        var exp = CCJSqlParserUtil.parseCondExpression("a.ID - b.ID + ID >= 3");
        var cond = new ConditionExpression(exp, "c");
        // 测试
        var params = new HashMap<ColumnIdentifier, Object>();
        params.put(new ColumnIdentifier("a", "ID"), 1);
        params.put(new ColumnIdentifier("b", "ID"), 2);
        params.put(new ColumnIdentifier("c", "ID"), 3);
        assertFalse(cond.evaluate(params));
        //
        params.replace(new ColumnIdentifier("b", "ID"), 1);
        assertTrue(cond.evaluate(params));
    }

    public void testEvaluateString() throws JSQLParserException {
        var exp = CCJSqlParserUtil.parseCondExpression("Name <= 'Kaaass'");
        var cond = new ConditionExpression(exp, "stu");
        // 测试
        var params = new HashMap<ColumnIdentifier, Object>();
        params.put(new ColumnIdentifier("stu", "Name"), "KAAAsS");
        assertTrue(cond.evaluate(params));
        //
        params.put(new ColumnIdentifier("stu", "Name"), "kaaass");
        assertFalse(cond.evaluate(params));
        //
        params.put(new ColumnIdentifier("stu", "Name"), "Kaaass");
        assertTrue(cond.evaluate(params));
    }

    public void testGetParams() throws JSQLParserException {
        var exp = CCJSqlParserUtil.parseExpression("a.ID - b.ID + ID >= 3");
        var cond = new ConditionExpression(exp, "c");
        var result = cond.getParams();
        log.info("Parsed: {}", result);
        // 比较结果
        var expected = new ArrayList<ColumnIdentifier>();
        assertTrue(result.contains(new ColumnIdentifier("a", "ID")));
        assertTrue(result.contains(new ColumnIdentifier("b", "ID")));
        assertTrue(result.contains(new ColumnIdentifier("c", "ID")));
    }
}