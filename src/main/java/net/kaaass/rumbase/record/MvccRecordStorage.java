package net.kaaass.rumbase.record;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.dataitem.IItemStorage;
import net.kaaass.rumbase.record.exception.RecordNotFoundException;
import net.kaaass.rumbase.record.exception.StorageCorruptedException;
import net.kaaass.rumbase.transaction.TransactionContext;
import net.kaaass.rumbase.transaction.TransactionIsolation;
import net.kaaass.rumbase.transaction.TransactionStatus;

import java.util.Optional;

/**
 * 实现MVCC并且通过transaction模块实现2PL的记录存储
 *
 * @author kaaass
 */
@Slf4j
public class MvccRecordStorage implements IRecordStorage {

    private IItemStorage storage;

    private String identifiedName;

    /**
     * 元数据缓存。仅在第一条metadata可见时有效
     */
    private byte[] metadataCache = null;

    @SneakyThrows
    @Override
    public long insert(TransactionContext txContext, byte[] rawData) {
        var data = new byte[rawData.length + 16];
        // 拼接data
        writeXmin(data, txContext.getXid());
        writeXmax(data, 0);
        writePayload(data, rawData);
        // 不用检查版本跳跃的原因是，插入本身不用；更新操作必定先删除，而删除检查
        // 插入记录
        return storage.insertItem(txContext, data);
    }

    @SneakyThrows
    @Override
    public Optional<byte[]> queryOptional(TransactionContext txContext, long recordId) throws RecordNotFoundException {
        // 串行化事务则申请读锁
        if (txContext.getIsolation() == TransactionIsolation.SERIALIZABLE) {
            txContext.sharedLock(recordId, this.identifiedName);
        }
        // 读取数据
        var data = storage.queryItemByUuid(recordId);
        // 检查可见性
        if (isVisible(txContext, data)) {
            return Optional.of(readPayload(data));
        } else {
            return Optional.empty();
        }
    }

    @SneakyThrows
    @Override
    public void delete(TransactionContext txContext, long recordId) {
        // 读取数据
        var data = storage.queryItemByUuid(recordId);
        // 判断可见性，若不可见直接返回
        if (!isVisible(txContext, data)) {
            return;
        }
        // 申请互斥锁
        txContext.exclusiveLock(recordId, this.identifiedName);
        // 是否已经被删除
        var xmax = readXmax(data);
        if (xmax == txContext.getXid()) {
            return;
        }
        // 版本跳跃检查
        if (isVersionSkip(txContext, data)) {
            log.info("记录 {} 发生版本跳跃", recordId);
            // TODO 回滚
            return;
        }
        // 更新记录
        writeXmax(data, txContext.getXid());
        storage.updateItemByUuid(txContext, recordId, data);
    }

    /**
     * TODO 判断可见性
     */
    private boolean isVisible(TransactionContext txContext, byte[] data) {
        var isolation = txContext.getIsolation();
        var xmin = readXmin(data);
        var xmax = readXmax(data);
        if (isolation == TransactionIsolation.READ_UNCOMMITTED) {
            // 读未提交：没被删除
            return xmax == 0;
        } else if (isolation == TransactionIsolation.READ_COMMITTED) {
            // 读已提交
            if (xmin == txContext.getXid() && xmax == 0) {
                // 事务自身创建
                return true;
            }
            // TODO
            return false;
        } else if (isolation == TransactionIsolation.REPEATABLE_READ) {

        } else if (isolation == TransactionIsolation.SERIALIZABLE) {
            // 可串行化：没被删除，依靠2PL保证事务性
            return xmax == 0;
        }
        return true;
    }

    /**
     * 判断版本跳跃
     */
    private boolean isVersionSkip(TransactionContext txContext, byte[] data) {
        var isolation = txContext.getIsolation();
        if (isolation != TransactionIsolation.REPEATABLE_READ) {
            // 只有可重复读需要检测版本跳跃
            // 序列化隔离度因为严格2PL，根本不出现版本跳跃
            return false;
        }
        var xmax = readXmax(data);
        // 事务已经提交且对当前事务不可见
        return statusOf(xmax, txContext) == TransactionStatus.COMMITTED
                && (xmax > txContext.getXid() || txContext.getSnapshot().contains(xmax));
    }

    @SneakyThrows
    @Override
    public byte[] getMetadata(TransactionContext txContext) {
        // TODO 申请全表锁，用this锁代替
        synchronized (this) {
            if (this.metadataCache != null) {
                return this.metadataCache;
            }
            // 读入记录数据
            var uuids = storage.getMetadata();
            for (int st = 0; st < uuids.length; st += 8) {
                var uuid = MvccUtil.readLong(uuids, st);
                Optional<byte[]> result;
                try {
                    result = queryOptional(txContext, uuid);
                } catch (RecordNotFoundException e) {
                    throw new StorageCorruptedException(1, e);
                }
                if (result.isPresent()) {
                    // 如果是第一个，更新cache
                    if (st == 0) {
                        this.metadataCache = result.get();
                    }
                    return result.get();
                }
            }
        }
        return new byte[0];
    }

    @Override
    public void setMetadata(TransactionContext txContext, byte[] metadata) {
        // TODO 申请全表锁，用this锁代替
        // 主要逻辑：为了保证事务性，使用UUID数组，结合可见性选择对应的记录。倒序记录便于存储。
        synchronized (this) {
            // 创建一条记录
            var newId = insert(txContext, metadata);
            // 保存记录至下层 metadata
            var oldMetadata = storage.getMetadata();
            var newMetadata = new byte[oldMetadata.length + 8];
            System.arraycopy(oldMetadata, 0, newMetadata, 8, oldMetadata.length);
            MvccUtil.writeLong(newMetadata, 0, newId);
            storage.setMetadata(txContext, newMetadata);
            // 缓存取消
            this.metadataCache = null;
        }
    }

    private static TransactionStatus statusOf(int xid, TransactionContext txContext) {
        return txContext.getManager().getContext(xid).getStatus();
    }

    private static boolean isFinished(int xid, TransactionContext txContext) {
        var status = statusOf(xid, txContext);
        return status == TransactionStatus.COMMITTED || status == TransactionStatus.ABORTED;
    }

    private static void writeXmin(byte[] data, int xmin) {
        MvccUtil.writeInt(data, 0, xmin);
    }

    private static void writeXmax(byte[] data, int xmax) {
        MvccUtil.writeInt(data, 4, xmax);
    }

    private static void writePayload(byte[] data, byte[] payload) {
        System.arraycopy(payload, 0, data, 8, payload.length);
    }

    private static int readXmin(byte[] data) {
        return MvccUtil.readInt(data, 0);
    }

    private static int readXmax(byte[] data) {
        return MvccUtil.readInt(data, 4);
    }

    private static byte[] readPayload(byte[] data) {
        var result = new byte[data.length - 8];
        System.arraycopy(data, 8, result, 0, result.length);
        return result;
    }
}