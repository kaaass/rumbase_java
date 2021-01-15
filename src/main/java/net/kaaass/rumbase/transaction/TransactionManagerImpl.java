package net.kaaass.rumbase.transaction;

import com.igormaznitsa.jbbp.io.JBBPBitInputStream;
import com.igormaznitsa.jbbp.io.JBBPByteOrder;
import com.igormaznitsa.jbbp.io.JBBPOut;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.page.Page;
import net.kaaass.rumbase.page.PageManager;
import net.kaaass.rumbase.page.PageStorage;
import net.kaaass.rumbase.page.exception.FileException;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 事务管理器的实现
 *
 * @author criki
 */
@Slf4j
public class TransactionManagerImpl implements TransactionManager {

    /**
     * 事务状态持久化文件名
     */
    private static final String LOG_FILE_NAME = "xid.log";
    /**
     * 每页最大事务状态数
     */
    private static final int TX_NUM_PER_PAGE = 2048;
    /**
     * 事务ID计数器
     */
    @Getter
    private final AtomicInteger SIZE;
    /**
     * 事务数量日志写入锁
     */
    private final Lock sizeWriteLock = new ReentrantLock();
    /**
     * 事务状态日志
     */
    private final PageStorage storage;

    /**
     * 事务缓存
     * <p>
     * TODO 实现事务缓存调度
     */
    private final Map<Integer, TransactionContext> txCache = new HashMap<>();

    /**
     * Mock事务管理器
     */
    public TransactionManagerImpl() throws FileException, IOException {
        if (new File(LOG_FILE_NAME).exists()) {
            // 初始化storage
            this.storage = PageManager.fromFile(LOG_FILE_NAME);
            // 从日志文件里获取事务数量
            JBBPBitInputStream stream = new JBBPBitInputStream(storage.get(0).getData());
            int size = stream.readInt(JBBPByteOrder.BIG_ENDIAN);
            log.info("Initial size : {}", size);
            // 初始化SIZE
            this.SIZE = new AtomicInteger(size);
        } else {
            // 初始化storage
            this.storage = PageManager.fromFile(LOG_FILE_NAME);
            // 初始化SIZE
            this.SIZE = new AtomicInteger(0);
        }

    }

    /**
     * 创建新事务
     *
     * @param isolation 事务隔离度
     * @return 事务对象
     */
    @Override
    public TransactionContext createTransactionContext(TransactionIsolation isolation) {
        // 获取最新事务id
        int xid = SIZE.incrementAndGet();

        // 生成快照
        List<Integer> snapshot = new ArrayList<>();

        synchronized (txCache) {
            for (TransactionContext context : txCache.values()) {
                snapshot.add(context.getXid());
            }
        }

        // 创建对象
        TransactionContext context = new TransactionContextImpl(xid, isolation, this, snapshot);
        // 加入缓存
        txCache.put(xid, context);

        // 进行持久化
        Page page = storage.get(0);
        page.pin();
        sizeWriteLock.lock();
        try {
            // 转换数据
            byte[] bytes = JBBPOut.BeginBin()
                    .Int(SIZE.get())
                    .End().toByteArray();
            // 写入数据
            page.writeData(bytes);
            // 刷新数据
            page.flush();

            // 写入日志中的事务隔离度
            writeTransactionIsolation(xid, isolation);
            // 更新日志中的事务状态
            changeTransactionStatus(xid, TransactionStatus.PREPARING);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            sizeWriteLock.unlock();
            page.unpin();
        }


        return context;
    }

    /**
     * 改变日志中的事务隔离度
     *
     * @param xid       事务id
     * @param isolation 事务隔离度
     */
    private void writeTransactionIsolation(int xid, TransactionIsolation isolation) {
        int pageId = xid / TX_NUM_PER_PAGE + 1;
        int offset = xid % TX_NUM_PER_PAGE * 2 + 1;

        log.info("Xid : {}", xid);
        log.info("Page id : {}", pageId);
        log.info("offset : {}", offset);
        Page page = storage.get(pageId);
        page.pin();
        try {
            byte[] data = new byte[1];
            data[0] = (byte) isolation.getIsolationId();
            page.patchData(offset, data);
            page.flush();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            page.unpin();
        }
    }

    /**
     * 改变日志中的事务状态
     *
     * @param xid    事务id
     * @param status 新事务状态
     */
    @Override
    public void changeTransactionStatus(int xid, TransactionStatus status) {
        int pageId = xid / TX_NUM_PER_PAGE + 1;
        int offset = xid % TX_NUM_PER_PAGE * 2;

        Page page = storage.get(pageId);
        page.pin();
        try {
            byte[] data = new byte[1];
            data[0] = status.getStatusId();
            page.patchData(offset, data);
            page.flush();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            page.unpin();
        }

        TransactionContext oldContext = txCache.get(xid);
        TransactionContext newContext = new TransactionContextImpl(oldContext.getXid(), oldContext.getIsolation(), oldContext.getManager(), oldContext.getSnapshot(), status);
        txCache.put(xid, newContext);
    }

    /**
     * 根据事务id获取事务上下文
     *
     * @param xid 事务id
     * @return 事务id为xid的事务上下文
     */
    @Override
    public TransactionContext getContext(int xid) {
        if (txCache.containsKey(xid)) {
            return txCache.get(xid);
        }

        int pageId = xid / TX_NUM_PER_PAGE + 1;
        int statusOffset = xid % TX_NUM_PER_PAGE * 2;
        int isolationOffset = xid % TX_NUM_PER_PAGE * 2 + 1;

        Page page = storage.get(pageId);
        page.pin();
        byte[] bytes = page.getDataBytes();
        byte statusId = bytes[statusOffset];
        byte isolationId = bytes[isolationOffset];

        TransactionStatus status = TransactionStatus.getStatusById(statusId);
        TransactionIsolation isolation = TransactionIsolation.getStatusById(isolationId);

        page.unpin();

        return new TransactionContextImpl(xid, isolation, this, status);
    }
}
