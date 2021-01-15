package net.kaaass.rumbase.parse;

import lombok.AllArgsConstructor;
import lombok.Data;

import java.util.List;

/**
 * SQL语法树：建表语句
 *
 * @author kaaass
 */
@Data
@AllArgsConstructor
public class CreateTableStatement implements ISqlStatement {

    /**
     * 创建表的表名
     */
    private String tableName;

    /**
     * 创建表中的列定义
     */
    private List<ColumnDefinition> columnDefinitions;

    /**
     * SQL语法树：列定义
     */
    @Data
    @AllArgsConstructor
    public static class ColumnDefinition {

        /**
         * 字段类型
         */
        private ColumnType columnType;

        /**
         * 字段名称
         */
        private String columnName;
    }

    /**
     * SQL语法树：字段类型
     */
    @Data
    @AllArgsConstructor
    public static class ColumnType {

        /**
         * 字段的SQL类型名，全小写
         */
        private String typeName;

        /**
         * 字段参数，如varchar(255)则为['255']
         */
        private List<String> arguments;
    }
}
