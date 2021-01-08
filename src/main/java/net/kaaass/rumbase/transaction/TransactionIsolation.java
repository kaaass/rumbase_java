package net.kaaass.rumbase.transaction;

import lombok.Getter;

/**
 * 事务隔离度
 *
 * @author criki
 */
public enum TransactionIsolation {
    READ_UNCOMMITTED(0),   //读未提交
    READ_COMMITTED(1),     //读已提交
    REPEATABLE_READ(2),    //重复读
    SERIALIZABLE(3);        //串行化

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
}
