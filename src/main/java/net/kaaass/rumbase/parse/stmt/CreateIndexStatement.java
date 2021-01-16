package net.kaaass.rumbase.parse.stmt;

import lombok.AllArgsConstructor;
import lombok.Data;
import net.kaaass.rumbase.parse.ColumnIdentifier;
import net.kaaass.rumbase.parse.ISqlStatement;
import net.kaaass.rumbase.parse.ISqlStatementVisitor;

import java.util.List;

/**
 * SQL语法树：创建索引语句
 *
 * @author kaaass
 */
@Data
@AllArgsConstructor
public class CreateIndexStatement implements ISqlStatement {

    /**
     * 待创建的索引名称
     */
    private String indexName;

    /**
     * 索引的目标表名称
     */
    private String tableName;

    /**
     * 索引的目标列
     */
    private List<ColumnIdentifier> columns;

    @Override
    public <T> T accept(ISqlStatementVisitor<T> visitor) {
        return visitor.visit(this);
    }
}
