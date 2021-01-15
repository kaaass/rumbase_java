package net.kaaass.rumbase.exception;
/**
 * 运行时错误基类
 * <p>
 * 详细的错误规范参考 {@link net.kaaass.rumbase.exception.RumbaseException}
 *
 * @author kaaass
 */
public class RumbaseRuntimeException extends RuntimeException {

    private int mainId;

    private int subId;

    /**
     * 构造Rumbase运行时异常
     *
     * @param mainId 主错误号
     * @param subId  子错误号
     * @param reason 错误原因
     */
    public RumbaseRuntimeException(int mainId, int subId, String reason) {
        super(String.format("E%d-%d: %s", mainId, subId, reason));
        this.mainId = mainId;
        this.subId = subId;
    }

    /**
     * 构造Rumbase运行时异常
     *
     * @param mainId 主错误号
     * @param subId  子错误号
     * @param reason 错误原因
     * @param cause  源错误
     */
    public RumbaseRuntimeException(int mainId, int subId, String reason, Throwable cause) {
        super(String.format("E%d-%d: %s", mainId, subId, reason), cause);
        this.mainId = mainId;
        this.subId = subId;
    }
}
