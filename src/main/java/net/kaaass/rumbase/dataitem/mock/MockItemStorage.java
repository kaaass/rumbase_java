package net.kaaass.rumbase.dataitem.mock;

import lombok.Data;
import net.kaaass.rumbase.dataitem.IItemStorage;
import net.kaaass.rumbase.dataitem.exception.PageCorruptedException;
import net.kaaass.rumbase.dataitem.exception.UUIDException;
import net.kaaass.rumbase.page.exception.PageException;
import net.kaaass.rumbase.recovery.IRecoveryStorage;
import net.kaaass.rumbase.transaction.TransactionContext;

import java.io.IOException;
import java.util.*;

/**
 * 数据项管理的Mock,进行数据项的增删改查
 *
 * @author kaito
 */
@Data
public class MockItemStorage implements IItemStorage {

    private String fileName;
    /**
     * 当前第一个空闲的页，用于插入时作为起始页来进行操作。
     */
    private int tempFreePage;
    /**
     * 表信息头对应的UUID
     */
    private long headerUuid;


    /**
     * 模拟的数据信息
     */
    private Map<Long, byte[]> maps;
    /**
     * 模拟的文件头信息
     */
    private byte[] meta;

    /**
     * 模拟的构造函数
     *
     * @param fileName     文件名
     * @param tempFreePage 当前第一个空闲页号
     * @param headerUuid   头信息对应UUID
     */
    public MockItemStorage(String fileName, int tempFreePage, long headerUuid) {
        this.fileName = fileName;
        this.tempFreePage = tempFreePage;
        this.headerUuid = headerUuid;
        maps = new HashMap<>();
        meta = new byte[1024];
    }

    public Map<Long, byte[]> getMap() {
        return maps;
    }

    /**
     * 通过已存在的文件名解析得到数据项管理器
     *
     * @param fileName 文件名
     * @return 数据项管理器
     */

    public static IItemStorage ofFile(String fileName) {
        // TODO: 实际通过文件名建立数据项管理器，还需要获取到文件头信息来解析得到可以插入的起始页
        return new MockItemStorage(fileName, 0, 0);
    }

    /**
     * 新建数据库，并写入表头
     *
     * @param fileName    文件名
     * @param tableHeader 表头信息
     * @return 返回数据项管理器
     */
    public static IItemStorage ofNewFile(TransactionContext txContext, String fileName, byte[] tableHeader) {
        // TODO: 因为是新建的文件，所以需要给文件头写入头信息数据。
        return new MockItemStorage(fileName, 0, 0);
    }


    @Override
    public void setMetaUuid(long uuid) {

    }

    @Override
    public IRecoveryStorage getRecoveryStorage() {
        return null;
    }

    @Override
    public int getMaxPageId() {
        return 0;
    }

    @Override
    public void flush(long uuid) {

    }

    @Override
    public long insertItem(TransactionContext txContext, byte[] item) {
        Random ran = new Random();
        long r = ran.nextLong();
        maps.put(r, item);
        return r;
    }

    @Override
    public long insertItemWithoutLog( byte[] item) {
        return 0;
    }

    @Override
    public void insertItemWithUuid(byte[] item, long uuid) {
        maps.put(uuid, item);
    }

    @Override
    public byte[] queryItemByUuid(long uuid) throws UUIDException {
        if (maps.containsKey(uuid)) {
            return maps.get(uuid);
        } else {
            throw new UUIDException(2);
        }
    }

    @Override
    public List<byte[]> listItemByPageId(int pageId) {
        return new ArrayList<>(maps.values());
    }

    @Override
    public void updateItemByUuid(TransactionContext txContext, long uuid, byte[] item) throws UUIDException {
        if (maps.containsKey(uuid)) {
            maps.put(uuid, item);
        } else {
            throw new UUIDException(2);
        }
    }

    @Override
    public byte[] updateItemWithoutLog(long uuid, byte[] item) throws UUIDException {
        return new byte[0];
    }

    @Override
    public byte[] getMetadata() {
        return meta;
    }

    @Override
    public long setMetadata(TransactionContext txContext, byte[] metadata) {
        this.meta = metadata;
        return 0;
    }

    @Override
    public long setMetadataWithoutLog(byte[] metadata) throws PageCorruptedException {

        return 0;
    }


    @Override
    public void removeItems(List<Long> uuids) {
        System.out.println("已经清除文件对应uuid的信息");
    }

    @Override
    public void deleteUuid(long uuid) throws IOException, PageException {

    }

    @Override
    public void flush() {
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        MockItemStorage that = (MockItemStorage) o;
        return tempFreePage == that.tempFreePage &&
                headerUuid == that.headerUuid &&
                Objects.equals(fileName, that.fileName) &&
                Objects.equals(maps, that.maps) &&
                Arrays.equals(meta, that.meta);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(fileName, tempFreePage, headerUuid, maps);
        result = 31 * result + Arrays.hashCode(meta);
        return result;
    }
}
