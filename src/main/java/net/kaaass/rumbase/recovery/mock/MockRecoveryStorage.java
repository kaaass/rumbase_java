package net.kaaass.rumbase.recovery.mock;

import net.kaaass.rumbase.recovery.IRecoveryStorage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class MockRecoveryStorage implements IRecoveryStorage {

    public MockRecoveryStorage() {
        bytes = new ArrayList<>();
    }

    /**
     * 模拟日志记录
     */
    public List<byte[]> bytes;

    @Override
    public void begin(int xid, List<Integer> snapshots) {
        String beginStr = "begin " + xid;
        bytes.add(beginStr.getBytes());
        String snapStr = "snap " + snapshots.toString();
        bytes.add(snapStr.getBytes());
    }

    @Override
    public void rollback(int xid) {
        String backStr = "abort " + xid;
        bytes.add(backStr.getBytes());
    }

    @Override
    public void commit(int xid) {
        String commitStr = "commit " + xid;
        bytes.add(commitStr.getBytes());
    }

    @Override
    public void insert(int xid, long uuid, byte[] item) {
        String insertStr = "insert " + xid + " " + uuid + " ";
        // 对控制语句、数据两部分进行合并得到最终日志记录
        byte[] first = insertStr.getBytes();
        byte[] result = Arrays.copyOf(first, first.length + item.length);
        System.arraycopy(item, 0, result, first.length, item.length);
        bytes.add(result);
    }

    @Override
    public void update(int xid, long uuid, byte[] item) {
        String updateStr = "update " + xid + " " + uuid + " ";
        // 对控制语句、数据两部分进行合并得到最终日志记录
        byte[] first = updateStr.getBytes();
        byte[] result = Arrays.copyOf(first, first.length + item.length);
        System.arraycopy(item, 0, result, first.length, item.length);
        bytes.add(result);
    }

    @Override
    public void updateMeta(int xid, long metaUUID) {
        String updateMetaStr = "meta " + xid + " " + metaUUID;
        bytes.add(updateMetaStr.getBytes());
    }

    @Override
    public List<byte[]> getContent() {
        return bytes;
    }


}
