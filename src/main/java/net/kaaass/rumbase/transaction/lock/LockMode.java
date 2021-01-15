package net.kaaass.rumbase.transaction.lock;

/**
 * 锁模式
 * <p>
 * 用于锁表中标识锁类型
 *
 * @author criki
 */
public enum LockMode {
    /**
     * 共享锁
     */
    SHARED,
    /**
     * 排他锁
     */
    EXCLUSIVE
}
