package net.kaaass.rumbase.parse;

/**
 * 标志用接口，用于标志SQL语句
 *
 * @author kaaass
 */
public interface ISqlStatement {

    <T> T accept(ISqlStatementVisitor<T> visitor);
}
