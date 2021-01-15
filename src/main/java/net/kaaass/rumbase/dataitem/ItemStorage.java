package net.kaaass.rumbase.dataitem;

import com.igormaznitsa.jbbp.JBBPParser;
import com.igormaznitsa.jbbp.mapper.Bin;
import net.kaaass.rumbase.dataitem.exception.UUIDException;
import net.kaaass.rumbase.page.Page;
import net.kaaass.rumbase.page.PageManager;
import net.kaaass.rumbase.page.PageStorage;
import net.kaaass.rumbase.page.exception.FileException;
import net.kaaass.rumbase.page.exception.PageException;
import net.kaaass.rumbase.recovery.IRecoveryStorage;
import net.kaaass.rumbase.transaction.TransactionContext;

import java.io.IOException;
import java.util.List;

/**
 * 数据项管理器的具体实现
 *
 * <p>
 *     每个表的头信息都是一致的，为
 *     |头信息标志位：1234(共4字节)|当前可用的第一个空闲页(2字节)|是否写入表头信息(1字节)，写入为123|头信息所对应的UUID(8字节)
 *
 *     同时
 * </p>
 * @author  kaito
 */
public class ItemStorage implements IItemStorage {

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
     * 内部维护一个对应该文件的页管理器
     */
    private PageStorage pageStorage;
    /**
     * 维护一个日志管理器
     */
    private IRecoveryStorage recoveryStorage;


    public ItemStorage(String fileName, int tempFreePage, long headerUuid,PageStorage pageStorage) {
        this.fileName = fileName;
        this.tempFreePage = tempFreePage;
        this.headerUuid = headerUuid;
        this.pageStorage = pageStorage;
    }

    /**
     * 判断有没有头部标志
     *
     * <p>
     *     获取表头的前四个字节数据，若是1234则表示是表头，解析后面的数据，否则就认为该表没有被初始化
     * </p>
     * @param header 第一页的Page对象
     * @return 是否是表的第一页
     */
    private static boolean checkTableHeader(Page header){
        var data = header.getData();
        byte[] flag = new byte[4];
        try {
            data.read(flag);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return flag[0] == 1 && flag[1] == 2 && flag[2] == 3 && flag[3] == 4;
    }

    /**
     *  打开相应的文件并读取头信息，如果没有对应的头信息就进行初始化
     * @param fileName 文件名
     * @return 解析或新建得到的数据项管理器对象
     */
    public static IItemStorage ofFile(String fileName) throws FileException, IOException, PageException {
        var pageStorage = PageManager.fromFile(fileName);
        var header = pageStorage.get(0);
        if (checkTableHeader(header)){
            // 如果表头标志存在，就解析对应表头信息
            var h = JBBPParser.prepare("long headerFlag;int tempFreePage;byte hasHeaderInfo;long headerUuid;").parse(header.getData()).mapTo(new TableHeader());
            return new ItemStorage(fileName,h.tempFreePage,h.headerUuid,pageStorage);
        }else {
            // 若表头标志不存在，就初始化对应的表信息。
            // 只初始化headerFlag和tempFreePage，表头信息位置统一由setMetadata来实现
            byte[] bytes = new byte[]{1,2,3,4,0,1};
            header.patchData(0,bytes);
            return new ItemStorage(fileName,1,0,pageStorage);
        }
    }

    /**
     * 创建新的表，初始化相应数据，并且保存头信息。
     * @param fileName 文件名
     * @param metadata 表头信息
     * @return 数据项管理器
     */
    public static IItemStorage ofNewFile(TransactionContext txContext ,String fileName,byte[] metadata) throws IOException, FileException, PageException {
        var pageStorage = ItemStorage.ofFile(fileName);
        pageStorage.setMetadata(txContext,metadata);
        return pageStorage;
    }

    private PageHeader getPageHeader(Page page) throws IOException {
        var data = page.getData();
        var pageHeader = JBBPParser.prepare("long pageFlag;int pageId;long lsn;int leftSpace;int recordNumber;" +
                "Item[recordNumber]{int size;long uuid;int offset;}").parse(data).mapTo(new PageHeader());
        return pageHeader;
    }


    @Override
    public long insertItem(TransactionContext txContext, byte[] item) throws IOException {
        var page = pageStorage.get(this.tempFreePage);
        var pageHeader = getPageHeader(page);

        return 0;
    }

    @Override
    public void insertItemWithUuid(TransactionContext txContext,byte[] item, long uuid) {

    }

    @Override
    public byte[] queryItemByUuid(long uuid) throws UUIDException {
        return new byte[0];
    }

    @Override
    public List<byte[]> listItemByPageId(int pageId) {
        return null;
    }

    @Override
    public void updateItemByUuid(TransactionContext txContext,long uuid, byte[] item) throws UUIDException {

    }

    @Override
    public byte[] getMetadata() {
        return new byte[0];
    }

    @Override
    public void setMetadata(TransactionContext txContext,byte[] metadata) {

    }

    @Override
    public void removeItems(List<Long> uuids) {

    }
}

/**
 * 表头
 */
class TableHeader {
    /**
     * 是否是表头的标志
     */
    @Bin
    long headerFlag;
    /**
     * 第一个可使用的空闲页编号
     */
    @Bin
    int tempFreePage;
    /**
     * 是否有表头信息
     */
    @Bin
    byte hasHeaderInfo;
    /**
     * 表头信息对应的UUID
     */
    @Bin
    long headerUuid;
}

/**
 * 每个数据项对应的相关信息
 */
class Item{
    /**
     * 数据项大小
     */
    @Bin
    int size;
    /**
     * 数据项编号
     */
    @Bin
    long uuid;
    /**
     * 页内偏移
     */
    @Bin
    int offset;
}

/**
 * 页头
 */
class PageHeader{
    /**
     * 页头标志
     */
    @Bin
    long pageFlag;
    /**
     * 当前页号
     */
    @Bin
    int pageId;
    /**
     * 日志记录编号
     */
    @Bin
    long lsn;
    /**
     * 剩余空间大小
     */
    @Bin
    int leftSpace;
    /**
     * 页内记录总数
     */
    @Bin
    int recordNumber;
    /**
     * 页内记录的相关信息
     */
    @Bin
    Item[] item;
}