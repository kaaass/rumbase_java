package net.kaaass.rumbase.record;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.dataitem.IItemStorage;
import net.kaaass.rumbase.dataitem.exception.UUIDException;
import net.kaaass.rumbase.record.exception.NeedRollbackException;
import net.kaaass.rumbase.record.exception.RecordNotFoundException;
import net.kaaass.rumbase.record.exception.StorageCorruptedException;
import net.kaaass.rumbase.transaction.TransactionContext;
import net.kaaass.rumbase.transaction.TransactionIsolation;
import net.kaaass.rumbase.transaction.TransactionStatus;
import net.kaaass.rumbase.transaction.exception.DeadlockException;

import java.util.Optional;

/**
 * 实现MVCC并且通过transaction模块实现2PL的记录存储
 *
 * @author kaaass
 */
@Slf4j
@RequiredArgsConstructor
public class MvccRecordStorage implements IRecordStorage {

    @NonNull
    private final IItemStorage storage;

    @NonNull
    @Getter
    private final String identifiedName;

    /**
     * 元数据缓存。仅在第一条metadata可见时有效
     */
    private byte[] metadataCache = null;

    @Override
    public long insert(TransactionContext txContext, byte[] rawData) {
        var data = new byte[rawData.length + 8];
        // 拼接data
        writeXmin(data, txContext.getXid());
        writeXmax(data, 0);
        writePayload(data, rawData);
        // 不用检查版本跳跃的原因是，插入本身不用；更新操作必定先删除，而删除检查
        // 插入记录
        return storage.insertItem(txContext, data);
    }

    @Override
    public Optional<byte[]> queryOptional(TransactionContext txContext, long recordId) throws RecordNotFoundException {
        // 串行化事务则申请读锁
        if (txContext.getIsolation() == TransactionIsolation.SERIALIZABLE) {
            try {
                txContext.sharedLock(recordId, this.identifiedName);
            } catch (DeadlockException e) {
                throw new NeedRollbackException(2, e);
            }
        }
        // 读取数据
        byte[] data;
        try {
            data = storage.queryItemByUuid(recordId);
        } catch (UUIDException e) {
            throw new RecordNotFoundException(1, e);
        }
        // 检查可见性
        if (isVisible(txContext, data)) {
            return Optional.of(readPayload(data));
        } else {
            return Optional.empty();
        }
    }

    @Override
    public void delete(TransactionContext txContext, long recordId) throws RecordNotFoundException {
        // 读取数据
        byte[] data;
        try {
            data = storage.queryItemByUuid(recordId);
        } catch (UUIDException e) {
            throw new RecordNotFoundException(1, e);
        }
        // 判断可见性，若不可见直接返回
        if (!isVisible(txContext, data)) {
            throw new RecordNotFoundException(2);
        }
        // 申请互斥锁
        try {
            txContext.exclusiveLock(recordId, this.identifiedName);
        } catch (DeadlockException e) {
            throw new NeedRollbackException(2, e);
        }
        var xid = txContext.getXid();
        if (xid == 0) {
            xid = -1;
        }
        // 是否已经被删除
        var xmax = readXmax(data);
        if (xmax == xid) {
            throw new RecordNotFoundException(2);
        }
        // 版本跳跃检查
        if (isVersionSkip(txContext, data)) {
            log.info("事务 {} 操作记录 {} 发生版本跳跃", txContext.getXid(), recordId);
            throw new NeedRollbackException(1);
        }
        // 更新记录
        writeXmax(data, xid);
        try {
            storage.updateItemByUuid(txContext, recordId, data);
        } catch (UUIDException e) {
            throw new RecordNotFoundException(1, e);
        }
    }

    /**
     * 判断可见性
     */
    private boolean isVisible(TransactionContext txContext, byte[] data) {
        var isolation = txContext.getIsolation();
        var xid = txContext.getXid();
        var xmin = readXmin(data);
        var xmax = readXmax(data);
        if (isolation == TransactionIsolation.READ_UNCOMMITTED) {
            // 读未提交：没被删除
            return xmax == 0;
        } else if (isolation == TransactionIsolation.READ_COMMITTED) {
            // 读已提交
            // 事务自身创建，且未被删除
            if (xmin == xid && xmax == 0) {
                return true;
            }
            // 记录已提交
            if (statusOf(xmin, txContext) == TransactionStatus.COMMITTED) {
                // 未被删除
                if (xmax == 0) {
                    return true;
                }
                // 被自身删除则不可见
                if (xmax == xid) {
                    return false;
                }
                // 被删除但是删除事务未提交
                return statusOf(xmax, txContext) != TransactionStatus.COMMITTED;
            }
            return false;
        } else if (isolation == TransactionIsolation.REPEATABLE_READ) {
            // 可重复读
            // 事务自身创建，且未被删除
            if (xmin == xid && xmax == 0) {
                return true;
            }
            // 记录被可见事务提交
            if (xmin < xid && !txContext.getSnapshot().contains(xmin)
                    && statusOf(xmin, txContext) == TransactionStatus.COMMITTED) {
                // 未被删除
                if (xmax == 0) {
                    return true;
                }
                // 被自身删除则不可见
                if (xmax == xid) {
                    return false;
                }
                // 被删除但是删除事务未提交
                return xmax > xid || txContext.getSnapshot().contains(xmax)
                        || statusOf(xmax, txContext) != TransactionStatus.COMMITTED;
            }
        } else if (isolation == TransactionIsolation.SERIALIZABLE) {
            // 可串行化：没被删除，依靠2PL保证事务性
            return xmax == 0;
        }
        return false;
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
        if (xid == 0 || xid == -1) {
            return TransactionStatus.COMMITTED;
        }
        return txContext.getManager().getContext(xid).getStatus();
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
