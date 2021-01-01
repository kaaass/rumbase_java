package net.kaaass.rumbase.record.exception;

import net.kaaass.rumbase.exception.RumbaseException;

/**
 * E5001 记录不存在异常
 * <p>
 * E5001-1 物理记录不存在
 */
public class RecordNotFoundException extends RumbaseException {
    /**
     * 记录不存在异常
     *
     * @param subId  子错误号
     * @param reason 错误原因
     */
    public RecordNotFoundException(int subId, String reason) {
        super(5001, subId, reason);
    }
}
