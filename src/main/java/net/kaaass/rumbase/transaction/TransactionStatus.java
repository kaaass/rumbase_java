package net.kaaass.rumbase.transaction;

import lombok.Getter;

/**
 * 事务状态
 *
 * @author criki
 */
public enum TransactionStatus {
    /**
     * 未开始
     */
    PREPARING(0),
    /**
     * 正在进行
     */
    ACTIVE(1),
    /**
     * 已提交
     */
    COMMITTED(2),
    /**
     * 被撤销
     */
    ABORTED(3);

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

    public static TransactionStatus getStatusById(byte id) {
        for (TransactionStatus value : values()) {
            if (id == value.statusId) {
                return value;
            }
        }

        return null;
    }
}
