package net.kaaass.rumbase.parse.parser;

import net.kaaass.rumbase.parse.ColumnIdentifier;
import net.sf.jsqlparser.schema.Column;

import java.util.List;
import java.util.stream.Collectors;

/**
 * 解析相关的工具函数
 *
 * @author kaaass
 */
public class ParserUtil {

    /**
     * 映射字段
     *
     * @param column           字段
     * @param defaultTableName 若不存在表，默认填充的表字段
     * @return 语法树字段
     */
    public static ColumnIdentifier mapColumn(Column column, String defaultTableName) {
        return new ColumnIdentifier(
                column.getTable() == null ? defaultTableName : column.getTable().getName(),
                column.getColumnName()
        );
    }

    /**
     * 映射字段列表
     *
     * @param columnList       字段列表
     * @param defaultTableName 若不存在表，默认填充的表字段
     * @return 语法树字段列表
     */
    public static List<ColumnIdentifier> mapColumnList(List<Column> columnList, String defaultTableName) {
        return columnList.stream()
                .map(column -> new ColumnIdentifier(
                        column.getTable() == null ? defaultTableName : column.getTable().getName(),
                        column.getColumnName()
                ))
                .collect(Collectors.toList());
    }
}
