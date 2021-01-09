package net.kaaass.rumbase.transaction;

import lombok.Getter;

/**
 * 事务状态
 *
 * @author criki
 */
public enum TransactionStatus {
    PREPARING(0),   //未开始
    ACTIVE(1),      //正在进行
    COMMITTED(2),   //已提交
    ABORTED(3);     //被撤销

    /**
     * 事务状态Id
     */
    @Getter
    private final byte statusId;

    /**
     * 事务状态
     *
     * @param statusId 事务状态Id
     */
    TransactionStatus(int statusId) {
        this.statusId = (byte) statusId;
    }
}
