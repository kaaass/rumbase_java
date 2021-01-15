package net.kaaass.rumbase.parse;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 * SQL语法树：插入语句
 *
 * @author kaaass
 */
@Data
@AllArgsConstructor
public class InsertStatement implements ISqlStatement {

    /**
     * 插入行的目标表名
     */
    private String tableName;

    /**
     * 插入数据对应的列，若为null则说明值为完整元组
     */
    private List<ColumnIdentifier> columns;

    /**
     * 插入的数据，以字符串表示
     */
    private List<String> values;
}
