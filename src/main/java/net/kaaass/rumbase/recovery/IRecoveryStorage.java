package net.kaaass.rumbase.recovery;

import java.util.List;

/**
 * 日志管理的接口，用来进行日志的写入和恢复
 * @author kaito
 */
public interface IRecoveryStorage {
    /**
     * 记录事务开始
     *
     * @param xid       事务编号
     * @param snapshots 快照集合
     */
    void begin(int xid, List<Integer> snapshots);

    /**
     * 记录事务失败回滚
     *
     * @param xid
     */
    void rollback(int xid);

    /**
     * 记录事务完成
     *
     * @param xid
     */
    void commit(int xid);

    /**
     * 插入数据项的日志记录
     *
     * @param xid  日志编号
     * @param uuid 数据项的对应编号
     * @param item 插入的数据内容
     */
    void insert(int xid, long uuid, byte[] item);

    /**
     * 更新数据项的日志记录
     *
     * @param xid
     * @param uuid
     * @param item
     */
    void update(int xid, long uuid, byte[] item);

    /**
     * 更新数据项的日志头
     *
     * @param xid
     * @param metaUUID 头信息的UUID
     */
    void updateMeta(int xid, long metaUUID);

    /**
     * 模拟打印日志资料
     */
    List<byte[]> getContent();
}
