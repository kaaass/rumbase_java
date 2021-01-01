package net.kaaass.rumbase.transaction;

/**
 * 事务上下文
 *
 * @author
 */
public class TransactionContext {

    private TransactionContext() {
    }

    /**
     * 返回空事务上下文。空事务或超级事务XID为0，不受事务限制，也不具备ACID性质。
     * @return 空事务上下文
     */
    public static TransactionContext empty() {
        return new TransactionContext();
    }
}
