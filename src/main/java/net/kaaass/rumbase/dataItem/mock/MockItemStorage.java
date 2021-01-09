package net.kaaass.rumbase.dataItem.mock;

import lombok.Data;
import net.kaaass.rumbase.dataItem.IItemStorage;
import net.kaaass.rumbase.dataItem.exception.UUIDException;

import java.util.*;

/**
 * 数据项管理的Mock,进行数据项的增删改查
 *
 * @author kaito
 * 
 */
@Data
public class MockItemStorage implements IItemStorage {

    private String fileName;
    /**
     * 当前第一个空闲的页，用于插入时作为起始页来进行操作。
     */
    private int tempFreePage;
    /**
     *  表信息头对应的UUID
     */
    private long headerUUID;


    // 模拟的数据信息
    private  Map<Long,byte[]> maps;
    // 模拟的文件头信息
    private byte[] meta;

    /**
     * 模拟的构造函数
     * @param fileName
     * @param tempFreePage
     * @param headerUUID
     */
    public MockItemStorage(String fileName, int tempFreePage, long headerUUID) {
        this.fileName = fileName;
        this.tempFreePage = tempFreePage;
        this.headerUUID = headerUUID;
        maps = new HashMap<>();
        meta = new byte[1024];
    }

    public Map<Long,byte[]> getMap(){
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
        return new MockItemStorage(fileName,0,0);
    }

    /**
     * 新建数据库，并写入表头
     *
     * @param fileName
     * @param tableHeader
     * @return
     */
    public static IItemStorage ofNewFile(String fileName, byte[] tableHeader){
        // TODO: 因为是新建的文件，所以需要给文件头写入头信息数据。
        return new MockItemStorage(fileName,0,0);
    }


    @Override
    public long insertItem(byte[] item) {
        Random ran = new Random();
        long r = ran.nextLong();
        maps.put(r,item);
        return r;
    }

    @Override
    public void insertItemWithUUID(byte[] item, long uuid) throws UUIDException {
        if (maps.containsKey(uuid)){
            throw new UUIDException(1);
        }else {
            maps.put(uuid,item);
        }
    }

    @Override
    public byte[] queryItemByUUID(long uuid) throws UUIDException {
        if (maps.containsKey(uuid)){
            return maps.get(uuid);
        }else{
            throw new UUIDException(2);
        }
    }

    @Override
    public List<byte[]> queryItemByPageID(int pageID) {
        List bytes = new ArrayList();
        for (var s:maps.values()){
            bytes.add(s);
        }
        return bytes;
    }

    @Override
    public void updateItemByUUID(long uuid, byte[] item) throws UUIDException {
        if (maps.containsKey(uuid)){
            maps.put(uuid,item);
        }else{
            throw new UUIDException(2);
        }
    }

    @Override
    public byte[] getMetadata() {
        return meta;
    }

    @Override
    public void setMetadata(byte[] metadata) {
        this.meta = metadata;
    }


    @Override
    public void removeItems(List<Long> uuids) {
        System.out.println("已经清除文件对应uuid的信息");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MockItemStorage that = (MockItemStorage) o;
        return tempFreePage == that.tempFreePage &&
                headerUUID == that.headerUUID &&
                Objects.equals(fileName, that.fileName) &&
                Objects.equals(maps, that.maps) &&
                Arrays.equals(meta, that.meta);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(fileName, tempFreePage, headerUUID, maps);
        result = 31 * result + Arrays.hashCode(meta);
        return result;
    }
}
