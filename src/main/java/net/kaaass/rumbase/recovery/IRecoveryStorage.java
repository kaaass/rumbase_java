package net.kaaass.rumbase.recovery;

import net.kaaass.rumbase.page.exception.FileException;

import java.io.IOException;
import java.util.List;

/**
 * 日志管理的接口，用来进行日志的写入和恢复
 *
 * @author kaito
 */
public interface IRecoveryStorage {
    /**
     * 记录事务开始
     *
     * @param xid       事务编号
     * @param snapshots 快照集合
     */
    void begin(int xid, List<Integer> snapshots) throws IOException, FileException;

    /**
     * 记录事务失败回滚
     *
     * @param xid
     */
    void rollback(int xid) throws IOException, FileException;

    /**
     * 记录事务完成
     *
     * @param xid
     */
    void commit(int xid) throws IOException, FileException;

    /**
     * 插入数据项的日志记录
     *
     * @param xid  日志编号
     * @param uuid 数据项的对应编号
     * @param item 插入的数据内容
     */
    void insert(int xid, long uuid, byte[] item) throws IOException, FileException;

    /**
     * 更新数据项的日志记录
     *
     * @param xid
     * @param uuid
     */
    void update(int xid, long uuid, byte[] item_before,byte[] item_after) throws IOException, FileException;

    /**
     * 更新数据项的日志头
     *
     * @param xid
     * @param metaUUID 头信息的UUID
     */
    void updateMeta(int xid, long metaUUID) throws IOException, FileException;

    /**
     * 模拟打印日志资料
     */
    List<byte[]> getContent();
}
