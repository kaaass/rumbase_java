package net.kaaass.rumbase.transaction.lock;

import lombok.Getter;

/**
 * 数据项标识符
 * <p>
 * 用于唯一指定一个数据项
 *
 * @author criki
 */
public class DataItemId {
    /**
     * 数据项所在表
     */
    @Getter
    private final String tableName;

    /**
     * 数据项表内id
     */
    @Getter
    private final long uuid;

    public DataItemId(String tableName, long uuid) {
        this.tableName = tableName;
        this.uuid = uuid;
    }

    @Override
    public String toString() {
        return "(" + tableName + ", " + uuid + ")";
    }

    @Override
    public int hashCode() {
        return tableName.hashCode() * 37 + Long.valueOf(uuid).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }

        if (this == obj) {
            return true;
        }

        if (!(obj instanceof DataItemId)) {
            return false;
        }

        DataItemId id = (DataItemId) obj;

        return this.tableName.equals(id.tableName)
                && this.uuid == id.uuid;
    }
}
