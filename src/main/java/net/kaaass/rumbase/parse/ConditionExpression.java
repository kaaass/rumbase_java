package net.kaaass.rumbase.parse;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.kaaass.rumbase.parse.exception.EvaluationException;
import net.kaaass.rumbase.parse.parser.jsqlp.ParserUtil;
import net.sf.jsqlparser.expression.*;
import net.sf.jsqlparser.expression.operators.arithmetic.*;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.util.deparser.ExpressionDeParser;

import java.util.*;

/**
 * 条件表达式操作相关
 *
 * @author kaaass
 */
@RequiredArgsConstructor
public class ConditionExpression {

    public static double PRECISION = 0.00001;

    private final Expression expression;

    @NonNull
    private final String defaultTableName;

    private Map<Column, ColumnIdentifier> paramColumn = null;

    /**
     * 根据参数列表求值
     *
     * @param paramMap 参数列表，其中参数必须是原生类型的装箱对象，如Integer、String
     */
    public boolean evaluate(Map<ColumnIdentifier, Object> paramMap) {
        if (expression == null) {
            return true;
        }
        updateParam();
        var parser = new DeParser(paramMap);
        expression.accept(parser);
        var result = parser.getResult();
        if (result instanceof Double) {
            return Math.abs((Double) result) > PRECISION;
        } else if (result instanceof Long) {
            return ((Long) result) != 0;
        } else if (result instanceof Boolean) {
            return (boolean) result;
        } else if (result instanceof String) {
            return !"".equals(result);
        }
        throw new EvaluationException(1);
    }

    /**
     * 获得表达式求值需要的参数
     */
    public List<ColumnIdentifier> getParams() {
        if (expression == null) {
            return List.of();
        }
        updateParam();
        return List.copyOf(paramColumn.values());
    }

    private void updateParam() {
        if (paramColumn == null) {
            paramColumn = new HashMap<>();
            expression.accept(new ExpressionVisitorAdapter() {

                @Override
                public void visit(Column column) {
                    paramColumn.put(column, ParserUtil.mapColumn(column, defaultTableName));
                }
            });
        }
    }

    @Override
    public String toString() {
        return "ConditionExpression{" +
                "expression=" + expression +
                ", defaultTableName='" + defaultTableName + '\'' +
                '}';
    }

    /**
     * 实现表达式执行的具体访问者
     */
    @RequiredArgsConstructor
    class DeParser extends ExpressionDeParser {

        /*
         * 类型：数字暂时都转为Double、Boolean、String
         */
        Stack<Object> stack = new Stack<>();

        @NonNull
        Map<ColumnIdentifier, Object> params;

        /**
         * 获得表达式求值结果
         */
        public Object getResult() {
            if (stack.size() != 1) {
                throw new EvaluationException(2);
            }
            return stack.get(0);
        }

        @Override
        public void visit(Addition addition) {
            super.visit(addition);
            // +
            var b = stack.pop();
            var a = stack.pop();
            assert a instanceof Double;
            assert b instanceof Double;
            stack.push((double) a + (double) b);
        }

        @Override
        public void visit(Division division) {
            super.visit(division);
            // /
            var b = stack.pop();
            var a = stack.pop();
            assert a instanceof Double;
            assert b instanceof Double;
            stack.push((double) a / (double) b);
        }

        @Override
        public void visit(Multiplication multiplication) {
            super.visit(multiplication);
            // *
            var b = stack.pop();
            var a = stack.pop();
            assert a instanceof Double;
            assert b instanceof Double;
            stack.push((double) a * (double) b);
        }

        @Override
        public void visit(Subtraction subtraction) {
            super.visit(subtraction);
            // -
            var b = stack.pop();
            var a = stack.pop();
            assert a instanceof Double;
            assert b instanceof Double;
            stack.push((double) a - (double) b);
        }

        @Override
        public void visit(SignedExpression signedExpression) {
            super.visit(signedExpression);
            //
            var a = stack.pop();
            assert a instanceof Double;
            if (signedExpression.getSign() == '+') {
                stack.push(a);
            } else if (signedExpression.getSign() == '-') {
                stack.push(-(double) a);
            } else if (signedExpression.getSign() == '~') {
                stack.push((double) (~(long) a));
            }
        }

        @Override
        public void visit(AndExpression andExpression) {
            super.visit(andExpression);
            // and
            var b = stack.pop();
            var a = stack.pop();
            assert a instanceof Boolean;
            assert b instanceof Boolean;
            stack.push((boolean) a && (boolean) b);
        }

        @Override
        public void visit(NotExpression notExpr) {
            super.visit(notExpr);
            // !
            var a = stack.pop();
            assert a instanceof Boolean;
            stack.push(!(boolean) a);
        }

        @Override
        public void visit(OrExpression orExpression) {
            super.visit(orExpression);
            // or
            var b = stack.pop();
            var a = stack.pop();
            assert a instanceof Boolean;
            assert b instanceof Boolean;
            stack.push((boolean) a || (boolean) b);
        }

        @Override
        public void visit(Between between) {
            super.visit(between);
            // between
            var c = stack.pop();
            var b = stack.pop();
            var a = stack.pop();
            Comparator cmp = Comparator.naturalOrder();
            stack.push(cmp.compare(b, a) <= 0 && cmp.compare(a, c) < 0);
        }

        @Override
        public void visit(GreaterThan greaterThan) {
            super.visit(greaterThan);
            // >
            var b = stack.pop();
            var a = stack.pop();
            Comparator cmp = Comparator.naturalOrder();
            stack.push(cmp.compare(a, b) > 0);
        }

        @Override
        public void visit(GreaterThanEquals greaterThanEquals) {
            super.visit(greaterThanEquals);
            // >=
            var b = stack.pop();
            var a = stack.pop();
            if (a instanceof Double && b instanceof Double) {
                stack.push((double) a - (double) b > -PRECISION);
            } else {
                Comparator cmp = Comparator.naturalOrder();
                stack.push(cmp.compare(a, b) >= 0);
            }
        }

        @Override
        public void visit(MinorThan minorThan) {
            super.visit(minorThan);
            // <
            var b = stack.pop();
            var a = stack.pop();
            Comparator cmp = Comparator.naturalOrder();
            stack.push(cmp.compare(a, b) < 0);
        }

        @Override
        public void visit(MinorThanEquals minorThanEquals) {
            super.visit(minorThanEquals);
            // <=
            var b = stack.pop();
            var a = stack.pop();
            if (a instanceof Double && b instanceof Double) {
                stack.push((double) a - (double) b < PRECISION);
            } else {
                Comparator cmp = Comparator.naturalOrder();
                stack.push(cmp.compare(a, b) <= 0);
            }
        }

        @Override
        public void visit(EqualsTo equalsTo) {
            super.visit(equalsTo);
            // =
            var b = stack.pop();
            var a = stack.pop();
            if (a instanceof Double && b instanceof Double) {
                stack.push(Math.abs((double) a - (double) b) < PRECISION);
            } else {
                stack.push(a.equals(b));
            }
        }

        @Override
        public void visit(NotEqualsTo notEqualsTo) {
            super.visit(notEqualsTo);
            // !=
            var b = stack.pop();
            var a = stack.pop();
            if (a instanceof Double && b instanceof Double) {
                stack.push(Math.abs((double) a - (double) b) >= PRECISION);
            } else {
                stack.push(!a.equals(b));
            }
        }

        @Override
        public void visit(IntegerDivision division) {
            super.visit(division);
            // //
            var b = stack.pop();
            var a = stack.pop();
            assert a instanceof Double;
            assert b instanceof Double;
            stack.push((double) ((long) a / (long) b));
        }

        @Override
        public void visit(BitwiseRightShift expr) {
            super.visit(expr);
            // >>
            var b = stack.pop();
            var a = stack.pop();
            assert a instanceof Double;
            assert b instanceof Double;
            stack.push((double) ((long) a >> (long) b));
        }

        @Override
        public void visit(BitwiseLeftShift expr) {
            super.visit(expr);
            // <<
            var b = stack.pop();
            var a = stack.pop();
            assert a instanceof Double;
            assert b instanceof Double;
            stack.push((double) ((long) a << (long) b));
        }

        @Override
        public void visit(BitwiseAnd bitwiseAnd) {
            super.visit(bitwiseAnd);
            // &
            var b = stack.pop();
            var a = stack.pop();
            assert a instanceof Double;
            assert b instanceof Double;
            stack.push((double) ((long) a & (long) b));
        }

        @Override
        public void visit(BitwiseOr bitwiseOr) {
            super.visit(bitwiseOr);
            // |
            var b = stack.pop();
            var a = stack.pop();
            assert a instanceof Double;
            assert b instanceof Double;
            stack.push((double) ((long) a | (long) b));
        }

        @Override
        public void visit(BitwiseXor bitwiseXor) {
            super.visit(bitwiseXor);
            // ^
            var b = stack.pop();
            var a = stack.pop();
            assert a instanceof Double;
            assert b instanceof Double;
            stack.push((double) ((long) a ^ (long) b));
        }

        @Override
        public void visit(Modulo modulo) {
            super.visit(modulo);
            // %
            var b = stack.pop();
            var a = stack.pop();
            assert a instanceof Double;
            assert b instanceof Double;
            stack.push((double) ((long) a % (long) b));
        }

        @Override
        public void visit(DoubleValue doubleValue) {
            super.visit(doubleValue);
            //
            stack.push(doubleValue.getValue());
        }

        @Override
        public void visit(HexValue hexValue) {
            super.visit(hexValue);
            //
            stack.push(Integer.valueOf(hexValue.getValue(), 16).doubleValue());
        }

        @Override
        public void visit(LongValue longValue) {
            super.visit(longValue);
            //
            stack.push((double) longValue.getValue());
        }

        @Override
        public void visit(NullValue nullValue) {
            super.visit(nullValue);
            //
            stack.push(null);
        }

        @Override
        public void visit(StringValue stringValue) {
            super.visit(stringValue);
            //
            stack.push(stringValue.getValue());
        }

        @Override
        public void visit(Column tableColumn) {
            super.visit(tableColumn);
            //
            var key = paramColumn.get(tableColumn);
            var value = params.get(key);
            if (value instanceof Number) {
                value = ((Number) value).doubleValue();
            }
            stack.push(value);
        }

        @Override
        public void visit(IsNullExpression isNullExpression) {
            super.visit(isNullExpression);
            //
            stack.push(stack.pop() == null);
        }

        @Override
        public void visit(IsBooleanExpression isBooleanExpression) {
            super.visit(isBooleanExpression);
            //
            stack.push(stack.pop() instanceof Boolean);
        }

        @Override
        public void visit(Concat concat) {
            super.visit(concat);
            //
            stack.push(stack.pop().toString() + stack.pop().toString());
        }
    }
}
