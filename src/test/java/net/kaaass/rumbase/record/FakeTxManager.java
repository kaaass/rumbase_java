package net.kaaass.rumbase.record;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.kaaass.rumbase.transaction.TransactionContext;
import net.kaaass.rumbase.transaction.TransactionIsolation;
import net.kaaass.rumbase.transaction.TransactionManager;
import net.kaaass.rumbase.transaction.TransactionStatus;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 用于测试可见性等的假事务管理器
 */
@RequiredArgsConstructor
public class FakeTxManager implements TransactionManager {

    private int txCount = 0;

    private final Map<Integer, FakeTxContext> txContextMap = new HashMap<>();

    @NonNull
    private final TransactionIsolation isolation;

    @Override
    public TransactionContext createTransactionContext(TransactionIsolation isolation) {
        var newTx = new FakeTxContext();
        newTx.setXid(++txCount);
        newTx.setIsolation(isolation);
        newTx.setManager(this);
        var snapshot =
                txContextMap.entrySet()
                        .stream()
                        .filter(tx -> tx.getValue().getStatus() == TransactionStatus.ACTIVE)
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toList());
        newTx.setSnapshot(snapshot);
        newTx.setStatus(TransactionStatus.ACTIVE);
        txContextMap.put(txCount, newTx);
        return newTx;
    }

    public TransactionContext begin() {
        return createTransactionContext(this.isolation);
    }

    public TransactionContext begin(int xid) {
        var newTx = new FakeTxContext();
        newTx.setXid(xid);
        newTx.setIsolation(this.isolation);
        newTx.setManager(this);
        var snapshot =
                txContextMap.entrySet()
                        .stream()
                        .filter(tx -> tx.getValue().getStatus() == TransactionStatus.ACTIVE)
                        .map(Map.Entry::getKey)
                        .collect(Collectors.toList());
        newTx.setSnapshot(snapshot);
        newTx.setStatus(TransactionStatus.ACTIVE);
        txContextMap.put(xid, newTx);
        return newTx;
    }

    @Override
    public TransactionContext getContext(int xid) {
        return txContextMap.get(xid);
    }

    @Override
    public void changeTransactionStatus(int xid, TransactionStatus status) {
        throw new UnsupportedOperationException();
    }
}
