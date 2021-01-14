package net.kaaass.rumbase.dataitem;

import com.igormaznitsa.jbbp.JBBPParser;
import com.igormaznitsa.jbbp.io.JBBPOut;
import com.igormaznitsa.jbbp.mapper.Bin;
import net.kaaass.rumbase.dataitem.exception.ItemException;
import net.kaaass.rumbase.dataitem.exception.UUIDException;
import net.kaaass.rumbase.page.Page;
import net.kaaass.rumbase.page.PageManager;
import net.kaaass.rumbase.page.PageStorage;
import net.kaaass.rumbase.page.exception.FileException;
import net.kaaass.rumbase.page.exception.PageException;
import net.kaaass.rumbase.recovery.IRecoveryStorage;
import net.kaaass.rumbase.transaction.TransactionContext;

import javax.swing.text.html.Option;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;

/**
 * 数据项管理器的具体实现
 *
 * <p>
 *     每个表的头信息都是一致的，为
 *     |头信息标志位：1234(共4字节)|当前可用的第一个空闲页(4字节)|是否写入表头信息(1字节)，写入为123|头信息所对应的UUID(8字节)
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
        header.pin();
        if (checkTableHeader(header)){
            // 如果表头标志存在，就解析对应表头信息
            var h = parseHeader(header);
            header.unpin();
            return new ItemStorage(fileName,h.tempFreePage,h.headerUuid,pageStorage);
        }else {
            // 若表头标志不存在，就初始化对应的表信息。
            // 只初始化headerFlag和tempFreePage，表头信息位置统一由setMetadata来实现
            var bytes = JBBPOut.BeginBin().
                    Byte(1,2,3,4).
                    Int(1).
                    End().toByteArray();
            header.patchData(0,bytes);
            header.unpin();
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

    private static int getRndByUuid(long uuid){
       return  (int)(uuid & 0xFFFFFFFF);
    }

    /**
     *  解析表头数据
     * @return 解析得到的表头对象
     *
     */
    private static TableHeader parseHeader(Page page) throws IOException {
        return JBBPParser.prepare("int headerFlag;int tempFreePage;byte hasHeaderInfo;long headerUuid;").
                parse(page.getData()).mapTo(new TableHeader());
    }

    /**
     *  通过偏移量解析得到数据
     * @param page 页
     * @param item 一个数据项记录
     * @return 解析得到的数据
     */
    private static byte[] parseData(Page page,Item item) throws IOException {
        int offset = item.offset;
        var data = page.getData();
        data.skip(offset);
        var content = JBBPParser.prepare("byte type;int size;byte[size] data;").parse(data).mapTo(new ItemContent());
        return content.data;
    }

    /**
     *  检查一个页头标志位是否存在，表示其是否已经被初始化。若初始化则标志位为2345
     * @param page 页
     * @return 该页是否已经被初始化
     * @throws IOException
     */
    private boolean checkPageHeader(Page page) throws IOException {
        byte[] pageFlag = new byte[4];
        var n = page.getData().read(pageFlag);
        return pageFlag[0] == 2 && pageFlag[1] == 3 && pageFlag[2] == 4 && pageFlag[3] == 5;
    }

    private Optional<PageHeader> getPageHeader(Page page) throws IOException {
        if (checkPageHeader(page)){
            // 如果页头信息正确，就解析得到页头信息的对象。
            var data = page.getData();
            var pageHeader = JBBPParser.prepare("int pageFlag;long lsn;int leftSpace;int recordNumber;" +
                    "item [recordNumber]{int uuid;int offset;}").parse(data);
            var p = pageHeader.mapTo(new PageHeader());
            return Optional.of(p);
        }else {
            // 否则返回空，交由上层处理。
            return Optional.empty();
        }
    }

    private PageHeader initPage(Page page) throws IOException, PageException {
        final byte[] bytes = JBBPOut.BeginBin().
                Byte(2,3,4,5). // 页头标志位
                Long(0).       // 日志记录位置，以后若有日志记录点则使用
                Int(4072).         // 剩余空间大小
                Int(0).        // 记录数目
                End().toByteArray();
        page.patchData(0,bytes);
        return new PageHeader(0,0,4072,0);
    }

    /**
     * 修改当前第一个可用页
     */
    private void addTempFreePage() throws IOException, PageException {
        this.tempFreePage += 1;
        var page = pageStorage.get(0);
        page.pin();
        var tempFreePage = JBBPOut.BeginBin().
                Int(this.tempFreePage)
                .End().toByteArray();
        page.patchData(4,tempFreePage);
        page.unpin();
    }

    @Override
    public synchronized long insertItem(TransactionContext txContext, byte[] item) throws IOException, PageException {
        var page = getPage(this.tempFreePage);
        var pageHeaderOp = getPageHeader(page);
        if (pageHeaderOp.isEmpty()){
            // 如果获取的页没有页头信息，则进行初始化。
            pageHeaderOp = Optional.of(initPage(page));
        }
        var pageHeader = pageHeaderOp.get();
        if (pageHeader.leftSpace - Math.min(item.length,MAX_RECORD_SIZE) <= MIN_LEFT_SPACE){
            // 如果剩余空间过小的话，就切换到下一个页进行，同时修改表头信息.并且，若数据过大则使用拉链，所以取512和数据大小较小的
            addTempFreePage();
            releasePage(page);
            return insertItem(txContext,item);
        }else {
            // 剩余空间足够，则插入
            int rnd = Math.abs(new Random().nextInt());
            long s= this.tempFreePage;
            long uuid = ((s << 32) + (long)(rnd));
            // 保证uuid不重复
            while (checkUuidExist(uuid)){
                rnd = Math.abs(new Random().nextInt());
                uuid = ((s << 32) + (long)(rnd));
            }
            insertToPage(page,pageHeader,txContext,item,rnd);
            releasePage(page);
            return uuid;
        }
    }

    /**
     * 将数据插入到页内对应位置，并修改页头信息
     */
    private  void insertToPage(Page page,PageHeader pageHeader,TransactionContext txContext,byte[] item,int rnd) throws IOException, PageException {
        if (item.length < MAX_RECORD_SIZE){
            int offset = 0 ;
            if (pageHeader.recordNumber == 0){
                // 如果页没有元素的话
                offset = 4095 - item.length - DATA_EXTRA_SIZE;
            }else {
                // 如果页内有插入的数据，则读取其offset并推算自己的offset
                offset = pageHeader.item[pageHeader.recordNumber-1].offset - item.length - DATA_EXTRA_SIZE;
            }

            //修改数据项头信息
            var i = JBBPOut.BeginBin().
                    Int(rnd).
                    Int(offset).
                    End().toByteArray();
            page.patchData(ITEM_OFFSET + pageHeader.recordNumber * ITEM_SIZE ,i);
            //修改数据信息
            var data = JBBPOut.BeginBin().
                    Byte(123).
                    Int(item.length).
                    Byte(item).End().toByteArray();
            page.patchData(offset,data);
            //修改页头信息
            var headerInfo = JBBPOut.BeginBin().
                    Int(pageHeader.leftSpace - item.length - DATA_EXTRA_SIZE).
                    Int(pageHeader.recordNumber + 1).
                    End().toByteArray();
            page.patchData(LEFT_SPACE_OFFSET,headerInfo);
        }
    }

    @Override
    public synchronized void insertItemWithUuid(TransactionContext txContext,byte[] item, long uuid) throws IOException, PageException {
        if (!checkUuidExist(uuid)){
            // 若不存在，则要恢复
            var page = getPage(uuid);
            var pageHeaderOp = getPageHeader(page);
            if (pageHeaderOp.isEmpty()){
                // 如果获取的页没有页头信息，则进行初始化。
                pageHeaderOp = Optional.of(initPage(page));
            }
            int rnd = getRndByUuid(uuid);
            insertToPage(page,pageHeaderOp.get(),txContext,item,rnd);
            releasePage(page);
        }
        // 若存在则不需要恢复，直接返回
    }

    /**
     * 根据uuid获取page
     */
    private Page getPage(long uuid){
        var pageId = uuid >> 32;
        Page page = pageStorage.get(pageId);
        page.pin();
        return page;
    }

    private Page getPage(int pageId){
        Page page = pageStorage.get(pageId);
        page.pin();
        return page;
    }

    /**
     * 释放page的操作
     * @param page
     */
    private void releasePage(Page page){
        page.unpin();
    }
    /**
     * 检查uuid是否存在
     * @param uuid
     * @return
     */
    private boolean checkUuidExist(long uuid) throws IOException {
        var page = getPage(uuid);
        int id = getRndByUuid(uuid);
        var pageHeader = getPageHeader(page);
        if (pageHeader.isEmpty()){
            return false;
        }else {
            for (var item : pageHeader.get().item){
                if (id == item.uuid){
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public byte[] queryItemByUuid(long uuid) throws UUIDException, IOException {
        if (checkUuidExist(uuid)){
            var page = getPage(uuid);
            var header = getPageHeader(page);
            if (header.isEmpty()){
                releasePage(page);
                throw new UUIDException(2);
            }else {
                // 遍历所有的item读取数据
                var items = header.get().item;
                int id = getRndByUuid(uuid);
                for (var item : items){
                    if (item.uuid == id){
                        var s =parseData(page,item);
                        releasePage(page);
                        return  s;
                    }
                }
            }
            return new byte[0];
        }else {
            throw new UUIDException(2);
        }
    }



    @Override
    public List<byte[]> listItemByPageId(int pageId) throws IOException {
        var page = getPage(pageId);
        List<byte[]> bytes = new ArrayList<>();
        var pageHeaderOp = getPageHeader(page);
        if (pageHeaderOp.isPresent()){
            var pageHeader = pageHeaderOp.get();
            for (var item : pageHeader.item){
                var data = parseData(page,item);
                bytes.add(data);
            }
        }
        releasePage(page);
        return bytes;
    }

    @Override
    public void updateItemByUuid(TransactionContext txContext,long uuid, byte[] item) throws UUIDException, IOException, PageException {
        var page = getPage(uuid);
        if (checkUuidExist(uuid)){
            var pageHeader = getPageHeader(page);
            var items = pageHeader.get().item;
            for(var i : items){
                if (i.uuid == getRndByUuid(uuid)){
                    var offset = i.offset;
                    page.patchData(offset,item);
                    releasePage(page);
                    return;
                }
            }
        }else{
            releasePage(page);
            throw new UUIDException(2);
        }
    }

    @Override
    public byte[] getMetadata() throws IOException, ItemException, UUIDException {
        var page = getPage(0);
        var header = parseHeader(page);
        if (checkTableHeader(page) && header.hasHeaderInfo == hasHeader){
            // 若表头已经被初始化并且有标志位的话，就说明有表头信息，进行获取.
            var h = queryItemByUuid(header.headerUuid);
            releasePage(page);
            return h;
        }else {
            throw new ItemException(1);
        }
    }

    @Override
    public void setMetadata(TransactionContext txContext,byte[] metadata) throws IOException, PageException {
        var page = getPage(0);
        var headerUuid = insertItem(txContext,metadata);
        var bytes = JBBPOut.BeginBin().
                Byte(hasHeader).
                Long(headerUuid).End().toByteArray();
        page.patchData(HeaderOffset,bytes);
        releasePage(page);
    }

    @Override
    public void removeItems(List<Long> uuids) {

    }

    /**
     * 头部标志的偏移
     */
    final static int HeaderOffset = 8;

    /**
     * 表头判断是否有表头数据
     */
    final static byte hasHeader = 123;
    /**
     * 数据项记录的大小
     */
    final static int ITEM_SIZE = 8;

    /**
     * 数据项记录的起始偏移
     */
    final static int ITEM_OFFSET = 20;
    /**
     * 记录数据项标志和数据项大小额外占用的空间
     */
    final static int DATA_EXTRA_SIZE = 5;
    /**
     * 页内空闲空间的页内偏移
     */
    final static int LEFT_SPACE_OFFSET = 12;
    /**
     * 页的大小
     */
    final static int PAGE_SIZE = 4096;
    /**
     * 单个数据项的最大值，超过的话使用拉链
     */
    final static int MAX_RECORD_SIZE = 512;
    /**
     * 每个页保留的大小
     */
    final static int MIN_LEFT_SPACE = 409;

    /**
     * 表头
     */
    public static class TableHeader {
        /**
         * 是否是表头的标志
         */
        @Bin
        int  headerFlag;
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

        public Object newInstance(Class<?> klazz){
            return klazz == TableHeader.class ? new TableHeader() : null;
        }
    }

    /**
     * 每个数据项对应的相关信息
     */
    public static class Item{
        /**
         * 数据项编号
         */
        @Bin
        public int uuid;
        /**
         * 页内偏移
         */
        @Bin
        public int offset;

        public Object newInstance(Class<?> klazz){
            return klazz == Item.class ? new Item() : null;
        }
    }

    /**
     * 页头
     */
    public static class PageHeader{
        /**
         * 页头标志
         */
        @Bin
        public int pageFlag;

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

        public PageHeader() { }

        public PageHeader(int pageFlag,long lsn, int leftSpace, int recordNumber) {
            this.pageFlag = pageFlag;
            this.lsn = lsn;
            this.leftSpace = leftSpace;
            this.recordNumber = recordNumber;
        }

        public Object newInstance(Class<?> klazz){
            return klazz == PageHeader.class ? new PageHeader() : null;
        }
    }

    public static class ItemContent{
        @Bin
        byte type;
        @Bin
        int size;
        @Bin
        byte[] data;
        public Object newInstance(Class<?> klazz){
            return klazz == ItemContent.class ? new ItemContent() : null;
        }
    }
}
