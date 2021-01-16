package net.kaaass.rumbase.transaction;

import lombok.Getter;

/**
 * 事务隔离度
 *
 * @author criki
 */
public enum TransactionIsolation {
    /**
     * 读未提交
     */
    READ_UNCOMMITTED(0),
    /**
     * 读已提交
     */
    READ_COMMITTED(1),
    /**
     * 重复读
     */
    REPEATABLE_READ(2),
    /**
     * 串行化
     */
    SERIALIZABLE(3);

    /**
     * 事务隔离度Id
     */
    @Getter
    private final int isolationId;

    /**
     * 事务隔离度
     *
     * @param isolationId 事务隔离度Id
     */
    TransactionIsolation(int isolationId) {
        this.isolationId = isolationId;
    }

    public static TransactionIsolation getStatusById(byte id) {
        for (TransactionIsolation value : values()) {
            if (id == value.isolationId) {
                return value;
            }
        }

        return null;
    }
}
