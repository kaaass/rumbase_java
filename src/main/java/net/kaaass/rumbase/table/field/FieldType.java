package net.kaaass.rumbase.table.field;

/**
 * 字段的类型枚举
 *
 * @author @KveinAxel
 */
public enum FieldType {
    /**
     * 4字节整型
     */
    INT,
    /**
     * 4字节浮点类型
     */
    FLOAT,
    /**
     * 带最大长度的可变长字符串
     */
    VARCHAR;

    @Override
    public String toString() {
        switch (this) {
            case INT:
                return "int";
            case FLOAT:
                return "float";
            case VARCHAR:
                return "varchar";
            default:
                return "";
        }
    }

}
