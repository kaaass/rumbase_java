package net.kaaass.rumbase.transaction.mock;

import net.kaaass.rumbase.transaction.TransactionContext;
import net.kaaass.rumbase.transaction.TransactionIsolation;
import net.kaaass.rumbase.transaction.TransactionManager;
import net.kaaass.rumbase.transaction.TransactionStatus;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Mock事务管理器
 *
 * @author criki
 */
@Deprecated
public class MockTransactionManager implements TransactionManager {

    /**
     * 事务状态持久化文件名
     */
    private static final String LOG_FILE_NAME = "xid.log";

    /**
     * Mock事务数量存储
     */
    public static int TransactionSize;

    /**
     * Mock事务状态持久化日志
     */
    public static Map<Integer, Byte> XidLog;

    static {
        XidLog = new HashMap<>();
    }

    /**
     * 事务ID计数器
     */
    private final AtomicInteger SIZE;

    /**
     * Mock事务管理器
     */
    public MockTransactionManager() {
        this.SIZE = new AtomicInteger(0);
    }

    /**
     * 创建新事务
     *
     * @param isolation 事务隔离度
     * @return 事务对象
     */
    @Override
    public TransactionContext createTransactionContext(TransactionIsolation isolation) {
        //获取最新事务id
        int xid = SIZE.incrementAndGet();
        //更新日志中的SIZE
        TransactionSize = SIZE.get();
        //更新日志中的事务状态
        changeTransactionStatus(xid, TransactionStatus.PREPARING);

        return new MockTransactionContext(xid, isolation, this);
    }

    /**
     * 根据事务id获取事务上下文
     *
     * @param xid 事务id
     * @return 事务id为xid的事务上下文
     */
    @Override
    public TransactionContext getContext(int xid) {
        TransactionStatus status = TransactionStatus.getStatusById(XidLog.get(xid));

        // 此处为了mock，事务隔离度均为TransactionIsolation.READ_UNCOMMITTED

        return new MockTransactionContext(xid, TransactionIsolation.READ_UNCOMMITTED, this, status);
    }

    /**
     * 改变日志中的事务状态
     *
     * @param xid    事务id
     * @param status 新事务状态
     */
    @Override
    public void changeTransactionStatus(int xid, TransactionStatus status) {
        XidLog.put(xid, status.getStatusId());
    }


}
