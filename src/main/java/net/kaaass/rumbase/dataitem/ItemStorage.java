package net.kaaass.rumbase.dataitem;

import com.igormaznitsa.jbbp.JBBPParser;
import com.igormaznitsa.jbbp.io.JBBPOut;
import com.igormaznitsa.jbbp.mapper.Bin;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.dataitem.exception.PageCorruptedException;
import net.kaaass.rumbase.dataitem.exception.UUIDException;
import net.kaaass.rumbase.page.Page;
import net.kaaass.rumbase.page.PageManager;
import net.kaaass.rumbase.page.PageStorage;
import net.kaaass.rumbase.page.exception.FileException;
import net.kaaass.rumbase.page.exception.PageException;
import net.kaaass.rumbase.recovery.IRecoveryStorage;
import net.kaaass.rumbase.recovery.RecoveryManager;
import net.kaaass.rumbase.recovery.exception.LogException;
import net.kaaass.rumbase.transaction.TransactionContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.Random;

/**
 * 数据项管理器的具体实现
 *
 * <p>
 * 每个表的头信息都是一致的，为
 * |头信息标志位：1 2 3 4(共4字节)|当前可用的第一个空闲页(4字节)|是否写入表头信息(1字节)，写入为123|头信息所对应的UUID(8字节)
 * <p>
 * 同时每个页都有相应的页头，页头格式为：
 * |页头标志位 2 3 4 5(共4字节)|lsn来记录日志相关内容(8字节)|页剩余空间大小(4字节)|页内数据项个数n(4字节)|每个数据项标志(n*8字节)|
 * <p>
 * 数据项标志为 |uuid后面的随机数(4字节)|在页内偏移offset(4字节)|
 * <p>
 * 每个数据项的内容为|标志位，表示有没有拉链等特殊情况(1字节)|数据长度m(4字节)|数据内容(m字节)|
 * TODO : |(若有拉链的话，则记录下一个uuid位置)8字节|
 * </p>
 *
 * @author kaito
 */
@Slf4j
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


    public ItemStorage(String fileName, int tempFreePage, long headerUuid, PageStorage pageStorage,IRecoveryStorage iRecoveryStorage) {
        this.fileName = fileName;
        this.tempFreePage = tempFreePage;
        this.headerUuid = headerUuid;
        this.pageStorage = pageStorage;
        this.recoveryStorage = iRecoveryStorage;
    }

    public ItemStorage(String fileName, int tempFreePage, long headerUuid, PageStorage pageStorage) {
        this.fileName = fileName;
        this.tempFreePage = tempFreePage;
        this.headerUuid = headerUuid;
        this.pageStorage = pageStorage;
    }

    /**
     * 判断有没有头部标志
     *
     * <p>
     * 获取表头的前四个字节数据，若是1234则表示是表头，解析后面的数据，否则就认为该表没有被初始化
     * </p>
     *
     * @param header 第一页的Page对象
     * @return 是否是表的第一页
     */
    private static boolean checkTableHeader(Page header) {
        var data = header.getData();
        byte[] flag = new byte[4];
        try {
            data.read(flag);
        } catch (IOException e) {
            throw new PageCorruptedException(1, e);
        }
        return flag[0] == 1 && flag[1] == 2 && flag[2] == 3 && flag[3] == 4;
    }

    /**
     * 打开相应的文件并读取头信息，如果没有对应的头信息就进行初始化
     *
     * @param fileName 文件名
     * @return 解析或新建得到的数据项管理器对象
     */
    public static IItemStorage ofFile(String fileName) throws FileException, PageException, IOException, LogException {
        var pageStorage = PageManager.fromFile(fileName);
        var header = pageStorage.get(0);
        header.pin();
        if (checkTableHeader(header)) {
            // 如果表头标志存在，就解析对应表头信息
            var h = parseTableHeader(header);
            header.unpin();
            var logStorage = RecoveryManager.getRecoveryStorage(fileName);
            return new ItemStorage(fileName, h.tempFreePage, h.headerUuid, pageStorage,logStorage);
        } else {
            // 若表头标志不存在，就初始化对应的表信息。
            // 只初始化headerFlag和tempFreePage，表头信息位置统一由setMetadata来实现
            byte[] bytes;
            try {
                bytes = JBBPOut.BeginBin().
                        Byte(1, 2, 3, 4).
                        Int(1).
                        End().toByteArray();
            } catch (IOException e) {
                header.unpin();
                throw new PageCorruptedException(1, e);
            }
            header.patchData(0, bytes);
            header.unpin();
            var logStorage = RecoveryManager.createRecoveryStorage(fileName);
            return new ItemStorage(fileName, 1, 0, pageStorage,logStorage);
        }
    }

    /**
     *  不使用日志打开文件
     */
    public static IItemStorage ofFileWithoutLog(String fileName) throws FileException, PageException, IOException, LogException {
        var pageStorage = PageManager.fromFile(fileName);
        var header = pageStorage.get(0);
        header.pin();
        if (checkTableHeader(header)) {
            // 如果表头标志存在，就解析对应表头信息
            var h = parseTableHeader(header);
            header.unpin();
            return new ItemStorage(fileName, h.tempFreePage, h.headerUuid, pageStorage);
        } else {
            // 若表头标志不存在，就初始化对应的表信息。
            // 只初始化headerFlag和tempFreePage，表头信息位置统一由setMetadata来实现
            byte[] bytes;
            try {
                bytes = JBBPOut.BeginBin().
                        Byte(1, 2, 3, 4).
                        Int(1).
                        End().toByteArray();
            } catch (IOException e) {
                header.unpin();
                throw new PageCorruptedException(1, e);
            }
            header.patchData(0, bytes);
            header.unpin();
            return new ItemStorage(fileName, 1, 0, pageStorage);
        }
    }

    /**
     * 创建新的表，初始化相应数据，并且保存头信息。
     *
     * @param fileName 文件名
     * @param metadata 表头信息
     * @return 数据项管理器
     */
    public static IItemStorage ofNewFile(TransactionContext txContext, String fileName, byte[] metadata) throws IOException, FileException, PageException, LogException {
        var pageStorage = ItemStorage.ofFile(fileName);
        pageStorage.setMetadata(txContext, metadata);
        return pageStorage;
    }

    /**
     * 不使用日志的创建文件
     */
    public static IItemStorage ofNewFileWithoutLog(String fileName, byte[] metadata) throws IOException, LogException, FileException, PageException {
        var pageStorage = ItemStorage.ofFileWithoutLog(fileName);
        pageStorage.setMetadataWithoutLog(metadata);
        return pageStorage;
    }

    /**
     * 根据uuid获取后面的随机位置
     */
    private static int getRndByUuid(long uuid) {
        return (int) (uuid & 0xFFFFFFFF);
    }

    /**
     * 根据uuid获取page
     */
    private Page getPage(long uuid) {
        var pageId = uuid >> 32;
        Page page = pageStorage.get(pageId);
        page.pin();
        return page;
    }

    /**
     * 根据pageId获取Page
     */
    private Page getPage(int pageId) {
        Page page = pageStorage.get(pageId);
        page.pin();
        return page;
    }

    /**
     * 释放page的操作
     *
     * @param page
     */
    private void releasePage(Page page) {
        page.unpin();
    }

    /**
     * 解析表头数据
     *
     * @return 解析得到的表头对象
     */
    private static TableHeader parseTableHeader(Page page) throws PageCorruptedException {
        try {
            return JBBPParser.prepare("int headerFlag;int tempFreePage;byte hasHeaderInfo;long headerUuid;").
                    parse(page.getData()).mapTo(new TableHeader());
        } catch (IOException e) {
            throw new PageCorruptedException(1, e);
        }
    }

    /**
     * 通过偏移量解析得到数据
     *
     * @param page 页
     * @param item 一个数据项记录
     * @return 解析得到的数据
     */
    private static byte[] parseData(Page page, Item item) throws PageCorruptedException {
        int offset = item.offset;
        var data = page.getData();
        try {
            data.skip(offset);
            var content = JBBPParser.prepare("byte type;int size;byte[size] data;").parse(data).mapTo(new ItemContent());
            if (content.type != NORMAL_DATA) {
                throw new PageCorruptedException(2);
            }
            return content.data;
        } catch (IOException e) {
            throw new PageCorruptedException(2, e);
        }
    }

    /**
     * 检查一个页头标志位是否存在，表示其是否已经被初始化。若初始化则标志位为2345
     *
     * @param page 页
     * @return 该页是否已经被初始化
     */
    private boolean checkPageHeader(Page page) {
        byte[] pageFlag = new byte[4];
        try {
            var n = page.getData().read(pageFlag);
        } catch (IOException e) {
            throw new PageCorruptedException(1, e);
        }
        return pageFlag[0] == 2 && pageFlag[1] == 3 && pageFlag[2] == 4 && pageFlag[3] == 5;
    }

    private Optional<PageHeader> getPageHeader(Page page) {
        try {
            if (checkPageHeader(page)) {
                // 如果页头信息正确，就解析得到页头信息的对象。
                var data = page.getData();
                var pageHeader = JBBPParser.prepare("int pageFlag;long lsn;int leftSpace;int recordNumber;" +
                        "item [recordNumber]{int uuid;int offset;}").parse(data);
                var p = pageHeader.mapTo(new PageHeader());
                return Optional.of(p);
            }
        } catch (IOException e) {
            throw new PageCorruptedException(1, e);
        }
        // 否则返回空，交由上层处理。
        return Optional.empty();
    }

    private PageHeader initPage(Page page) {
        final byte[] bytes;
        try {
            bytes = JBBPOut.BeginBin().
                    Byte(2, 3, 4, 5). // 页头标志位
                    Long(0).       // 日志记录位置，以后若有日志记录点则使用
                    Int(4072).         // 剩余空间大小
                    Int(0).        // 记录数目
                    End().toByteArray();
            page.patchData(0, bytes);
        } catch (IOException | PageException e) {
            throw new PageCorruptedException(1, e);
        }
        return new PageHeader(0, 0, 4072, 0);
    }

    /**
     * 修改当前第一个可用页
     */
    private void addTempFreePage() {
        this.tempFreePage += 1;
        var page = pageStorage.get(0);
        page.pin();
        byte[] tempFreePage;
        try {
            tempFreePage = JBBPOut.BeginBin().
                    Int(this.tempFreePage)
                    .End().toByteArray();
            page.patchData(4, tempFreePage);
        } catch (Exception e) {
            throw new PageCorruptedException(1, e);
        } finally {
            page.unpin();
        }
    }

    @Override
    public void setMetaUuid(long uuid) throws IOException, PageException {
        var page = getPage(0);
        try {
            var bytes = JBBPOut.BeginBin().
                    Byte(HAS_HEADER).
                    Long(uuid).End().toByteArray();
            page.patchData(HEADER_OFFSET, bytes);
        }finally {
            releasePage(page);
        }

    }

    @Override
    public IRecoveryStorage getRecoveryStorage() {
        return this.recoveryStorage;
    }

    @Override
    public int getMaxPageId() {
        var page = getPage(0);
        try{
            var header = parseTableHeader(page);
            return header.tempFreePage;
        }finally {
            releasePage(page);
        }
    }

    @Override
    public void flush(long uuid) throws FileException {
        var page = getPage(uuid);
        releasePage(page);
        page.flush();
    }

    @Override
    public synchronized long insertItem(TransactionContext txContext, byte[] item) throws IOException, FileException {
        long uuid = insertItemWithoutLog(item);
        recoveryStorage.insert(txContext.getXid(),uuid,item);
        return uuid;
    }
    @Override
    public  synchronized long insertItemWithoutLog(byte[] item) {
        var page = getPage(this.tempFreePage);
        try {
            var pageHeaderOp = getPageHeader(page);
            if (pageHeaderOp.isEmpty()) {
                // 如果获取的页没有页头信息，则进行初始化。
                pageHeaderOp = Optional.of(initPage(page));
            }
            var pageHeader = pageHeaderOp.get();
            if (pageHeader.leftSpace - Math.min(item.length, MAX_RECORD_SIZE) <= MIN_LEFT_SPACE) {
                // 如果剩余空间过小的话，就切换到下一个页进行，同时修改表头信息.并且，若数据过大则使用拉链，所以取512和数据大小较小的
                addTempFreePage();
                return insertItemWithoutLog(item);
            } else {
                // 剩余空间足够，则插入
                int rnd = Math.abs(new Random().nextInt());
                long s = this.tempFreePage;
                long uuid = ((s << 32) + (long) (rnd));
                // 保证uuid不重复
                while (checkUuidExist(uuid)) {
                    rnd = Math.abs(new Random().nextInt());
                    uuid = ((s << 32) + (long) (rnd));
                }
                insertToPage(page, pageHeader,item, rnd);
                return uuid;
            }
        } catch (PageCorruptedException e) {
            throw e;
        } catch (Exception e) {
            throw new PageCorruptedException(3);
        } finally {
            releasePage(page);
        }
    }

    /**
     * 将数据插入到页内对应位置，并修改页头信息
     */
    private void insertToPage(Page page, PageHeader pageHeader,byte[] item, int rnd) {
        if (item.length < MAX_RECORD_SIZE) {
            int offset = 0;
            if (pageHeader.recordNumber == 0) {
                // 如果页没有元素的话
                offset = 4095 - item.length - DATA_EXTRA_SIZE;
            } else {
                // 如果页内有插入的数据，则读取其offset并推算自己的offset
                offset = pageHeader.item[pageHeader.recordNumber - 1].offset - item.length - DATA_EXTRA_SIZE;
            }
            try {
                //修改数据项头信息
                var i = JBBPOut.BeginBin().
                        Int(rnd).
                        Int(offset).
                        End().toByteArray();
                page.patchData(ITEM_OFFSET + pageHeader.recordNumber * ITEM_SIZE, i);
                //修改数据信息
                var data = JBBPOut.BeginBin().
                        Byte(NORMAL_DATA).
                        Int(item.length).
                        Byte(item).End().toByteArray();
                page.patchData(offset, data);
            } catch (IOException | PageException e) {
                throw new PageCorruptedException(2, e);
            }
            try {
                //修改页头信息
                var headerInfo = JBBPOut.BeginBin().
                        Int(pageHeader.leftSpace - item.length - DATA_EXTRA_SIZE - ITEM_SIZE).
                        Int(pageHeader.recordNumber + 1).
                        End().toByteArray();
                page.patchData(LEFT_SPACE_OFFSET, headerInfo);
            } catch (IOException | PageException e) {
                throw new PageCorruptedException(1, e);
            }
        }
    }

    @Override
    public synchronized void insertItemWithUuid(byte[] item, long uuid) {
        if (!checkUuidExist(uuid)) {
            // 若不存在，则要恢复
            var page = getPage(uuid);
            try {
                var pageHeaderOp = getPageHeader(page);
                if (pageHeaderOp.isEmpty()) {
                    // 如果获取的页没有页头信息，则进行初始化。
                    pageHeaderOp = Optional.of(initPage(page));
                }
                int rnd = getRndByUuid(uuid);
                insertToPage(page, pageHeaderOp.get(), item, rnd);
            } catch (Exception e) {
                throw new PageCorruptedException(3);
            } finally {
                releasePage(page);
            }
        }
        // 若存在则不需要恢复，直接返回
    }

    /**
     * 日志恢复时用，回退对应的insert操作
     * @param uuid
     */
    @Override
    public void deleteUuid(long uuid) throws IOException, PageException {
        var page = getPage(uuid);
        try {
            var rnd = getRndByUuid(uuid);
            var header = getPageHeader(page);
            if (header.isPresent()){
                var items = header.get().item;
                for (int i = 0;i < items.length ; i ++){
                    if (rnd == items[i].uuid){
                        // 该item对应的起止位置,将其数据项变为-1
                        int offset = ITEM_OFFSET + i * ITEM_SIZE;
                        var bytes = JBBPOut.BeginBin().Int(-1).Int(0).End().toByteArray();
                        page.patchData(offset,bytes);
                        return;
                    }
                }
            }
        }finally {
            releasePage(page);
        }
    }

    /**
     * 检查uuid是否存在,若Uuid的页号超过当前可用页，则直接返回False
     *
     * @param uuid
     * @return
     */
    private boolean checkUuidExist(long uuid) {
        var pageId = uuid >> 32;
        if (pageId > this.tempFreePage || pageId < 0) {
            return false;
        }
        var page = getPage(uuid);
        int id = getRndByUuid(uuid);
        try {
            var pageHeader = getPageHeader(page);
            if (pageHeader.isEmpty()) {
                return false;
            } else {
                for (var item : pageHeader.get().item) {
                    if (id == item.uuid) {
                        return true;
                    }
                }
            }
            return false;
        } catch (Exception e) {
            throw new PageCorruptedException(2);
        } finally {
            releasePage(page);
        }
    }

    @Override
    public byte[] queryItemByUuid(long uuid) throws UUIDException {
        if (checkUuidExist(uuid)) {
            var page = getPage(uuid);
            try {
                var header = getPageHeader(page);
                if (header.isEmpty()) {
                    throw new UUIDException(2);
                } else {
                    // 遍历所有的item读取数据
                    var items = header.get().item;
                    int id = getRndByUuid(uuid);
                    for (var item : items) {
                        if (item.uuid == id) {
                            var s = parseData(page, item);
                            return s;
                        }
                    }
                }
                return new byte[0];
            } catch (Exception e) {
                throw new UUIDException(2);
            } finally {
                releasePage(page);
            }
        } else {
            throw new UUIDException(2);
        }
    }


    @Override
    public List<byte[]> listItemByPageId(int pageId) {
        var page = getPage(pageId);
        try {
            List<byte[]> bytes = new ArrayList<>();
            var pageHeaderOp = getPageHeader(page);
            if (pageHeaderOp.isPresent()) {
                var pageHeader = pageHeaderOp.get();
                for (var item : pageHeader.item) {
                    if (item.uuid < 0){
                        continue;
                    }else {
                        var data = parseData(page, item);
                        bytes.add(data);
                    }
                }
            }
            return bytes;
        } catch (Exception e) {
            throw new PageCorruptedException(2);
        } finally {
            releasePage(page);
        }
    }

    @Override
    public void updateItemByUuid(TransactionContext txContext, long uuid, byte[] item) throws UUIDException, PageCorruptedException, FileException, IOException {
        var item_before = updateItemWithoutLog(uuid, item);
        recoveryStorage.update(txContext.getXid(),uuid,item_before,item);
    }

    @Override
    public byte[] updateItemWithoutLog(long uuid, byte[] item) throws UUIDException {
        var page = getPage(uuid);
        try {
            if (checkUuidExist(uuid)) {
                // 若uuid存在
                var pageHeader = getPageHeader(page);
                var items = pageHeader.get().item;
                try {
                    for (var i : items) {
                        if (i.uuid == getRndByUuid(uuid)) {
                            var item_before = parseData(page,i);
                            var offset = i.offset;
                            var bytes = JBBPOut.BeginBin().
                                    Byte(NORMAL_DATA).
                                    Int(item.length)
                                    .Byte(item)
                                    .End().toByteArray();
                            page.patchData(offset, bytes);
                            return item_before;
                        }
                    }
                } catch (PageException | IOException e) {
                    throw new PageCorruptedException(2, e);
                }
            } else {
                // uuid不存在一般是恢复的时候 前面事务被取消.
                throw new UUIDException(2);
            }
        } finally {
            releasePage(page);
        }
        return new byte[1];
    }

    @Override
    public byte[] getMetadata() {
        var page = getPage(0);
        var header = parseTableHeader(page);
        if (checkTableHeader(page) && header.hasHeaderInfo == HAS_HEADER) {
            // 若表头已经被初始化并且有标志位的话，就说明有表头信息，进行获取.
            byte[] h;
            try {
                h = queryItemByUuid(header.headerUuid);
            } catch (UUIDException e) {
                // 若UUID不存在，肯定是页头的问题，因为控制过程都是ItemStorage操作的
                throw new PageCorruptedException(1, e);
            } finally {
                releasePage(page);
            }
            return h;
        } else {
            // 默认Metadata为空
            releasePage(page);
            return new byte[0];
        }
    }

    @Override
    public long setMetadata(TransactionContext txContext, byte[] metadata) throws PageCorruptedException, IOException, FileException {
        var page = getPage(0);
        try{
            var header = parseTableHeader(page);
            var uuid = setMetadataWithoutLog(metadata);
            recoveryStorage.updateMeta(txContext.getXid(),header.headerUuid,metadata);
            return uuid;
        }finally {
            releasePage(page);
        }

    }

    @Override
    public long setMetadataWithoutLog(byte[] metadata) throws PageCorruptedException {
        var page = getPage(0);
        try {
            var headerUuid = insertItemWithoutLog(metadata);
            var bytes = JBBPOut.BeginBin().
                    Byte(HAS_HEADER).
                    Long(headerUuid).End().toByteArray();
            page.patchData(HEADER_OFFSET, bytes);
            return headerUuid;
        } catch (Exception e) {
            throw new PageCorruptedException(1, e);
        } finally {
            releasePage(page);
        }
    }

    @Override
    public void removeItems(List<Long> uuids) {

    }

    /**
     *
     */
    final static byte NORMAL_DATA = 121;
    /**
     * 头部标志的偏移
     */
    final static int HEADER_OFFSET = 8;

    /**
     * 表头判断是否有表头数据
     */
    final static byte HAS_HEADER = 123;
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
        int headerFlag;
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

        public Object newInstance(Class<?> klazz) {
            return klazz == TableHeader.class ? new TableHeader() : null;
        }
    }

    /**
     * 每个数据项对应的相关信息
     */
    public static class Item {
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

        public Object newInstance(Class<?> klazz) {
            return klazz == Item.class ? new Item() : null;
        }
    }

    /**
     * 页头
     */
    public static class PageHeader {
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

        public PageHeader() {
        }

        public PageHeader(int pageFlag, long lsn, int leftSpace, int recordNumber) {
            this.pageFlag = pageFlag;
            this.lsn = lsn;
            this.leftSpace = leftSpace;
            this.recordNumber = recordNumber;
        }

        public Object newInstance(Class<?> klazz) {
            return klazz == PageHeader.class ? new PageHeader() : null;
        }
    }

    public static class ItemContent {
        @Bin
        byte type;
        @Bin
        int size;
        @Bin
        byte[] data;

        public Object newInstance(Class<?> klazz) {
            return klazz == ItemContent.class ? new ItemContent() : null;
        }
    }
}
