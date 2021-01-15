package net.kaaass.rumbase.index.btree;

import net.kaaass.rumbase.index.Index;
import net.kaaass.rumbase.index.Pair;
import net.kaaass.rumbase.index.exception.ItemInNextPageException;
import net.kaaass.rumbase.index.exception.PageFullException;
import net.kaaass.rumbase.index.exception.PageTypeException;
import net.kaaass.rumbase.page.Page;
import net.kaaass.rumbase.page.PageManager;
import net.kaaass.rumbase.page.PageStorage;
import net.kaaass.rumbase.page.exception.FileException;
import net.kaaass.rumbase.page.exception.PageException;

import javax.management.loading.MLet;
import java.nio.file.attribute.UserDefinedFileAttributeView;
import java.util.*;
import java.util.concurrent.locks.ReadWriteLock;

/**
 * @author 无索魏
 */
public class BPlusTreeIndex implements Index {
    Page root;
    PageStorage pageStorage;

    private enum PageType {  //页类型
        INTERNAL,
        //树枝节点
        LEAF,
        //树叶节点
        META;
        //元信息节点
    }

    /**
     * 内存中已经加载的的BPlusTreeIndex
     */

    public static Map<String, BPlusTreeIndex> B_PLUS_TREE_INDEX_MAP = new HashMap<>();
    public static int MAX_PAGE_ITEM = (4096 - 24)/16;
    public static int PAGE_BEGIN_POSTION = 24;
    public static int PAGE_MID_POSTION = PAGE_BEGIN_POSTION + 16 * (BPlusTreeIndex.MAX_PAGE_ITEM)/2 ;

    private Map<Long, ReadWriteLock> RW_LOCKS = new HashMap<>();

    public BPlusTreeIndex(String indexName) {
        try {
            pageStorage = PageManager.fromFile(indexName);
        } catch (FileException e) {
            e.printStackTrace();
        }
        root = pageStorage.get(4);
    }

    synchronized public void initPage() {
        initRootLeaf(root);
        Page metaPage = this.pageStorage.get(0);
        setPageType(metaPage,PageType.META);
        byte[] bs = ByteUtil.long2Bytes(5);
        try {
            metaPage.patchData(4,bs);
        } catch (PageException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void replace(Map<Long, Long> uuidMap) {
        //TODO
    }

    @Override
    synchronized public void insert(long dataHash, long uuid) {
        Stack<Long> pageStack = new Stack<>();
        Page currentPage = this.root;
        while (getPageType(currentPage) != PageType.LEAF) {
            long nextPageNum = 0;

            try {
                nextPageNum = queryFirstInternalItem(currentPage,dataHash);
            } catch (ItemInNextPageException e) {
                currentPage = this.pageStorage.get(e.getNextPageNum());
                continue;
            }

            pageStack.add(nextPageNum);
            currentPage = this.pageStorage.get(nextPageNum);
        }
        boolean isInsert = false;
        while (!isInsert) {

            try {
                insertLeafItem(currentPage, dataHash, uuid);
                isInsert = true;
            } catch (ItemInNextPageException e) {
                currentPage = this.pageStorage.get(e.getNextPageNum());
            } catch (PageFullException e) {
                long splitPageNum = 0;
                if (pageStack.size() != 0) {
                     splitPageNum = pageStack.pop();
                }
                long rawPageNum = this.getRawPageNum();
                Page rawPage = this.pageStorage.get(rawPageNum);
                long newKey = insertFullLeaf(currentPage, rawPage, rawPageNum, dataHash, uuid);
                boolean isInsertInternal = false, stackNeed = true;
                Page parent = null;
                long parentNum = 0;
                while (!isInsertInternal) {
                    if (stackNeed) {
                        if (pageStack.size() > 0) {
                            parentNum = pageStack.pop();
                            parent = this.pageStorage.get(parentNum);
                        } else {
                            if (getPageType(root) == PageType.LEAF) {
                                long rawPageNum0 = this.getRawPageNum();
                                Page rawPage0 = this.pageStorage.get(rawPageNum0);

                                try {
                                    rawPage0.writeData(root.getDataBytes());
                                } catch (PageException pageException) {
                                    pageException.printStackTrace();
                                }

                                initRootInternal(root, newKey, rawPageNum0, rawPageNum);
                            } else {

                                try {
                                    insertInternalItem(root, getMaxKey(rawPage), splitPageNum, rawPageNum, newKey);
                                } catch (PageFullException pageFullException) {
                                    long rawPageNum0 = this.getRawPageNum();
                                    Page rawPage0 = this.pageStorage.get(rawPageNum0);
                                    long rawPageNum1 = this.getRawPageNum();
                                    Page rawPage1 = this.pageStorage.get(rawPageNum1);
                                    try {
                                        rawPage0.writeData(root.getDataBytes());
                                    } catch (PageException pageException) {
                                        pageException.printStackTrace();
                                    }

                                    newKey = insertFullInternal(rawPage0, rawPage1, rawPageNum1, getMaxKey(rawPage), splitPageNum, rawPageNum, newKey);
                                    initRootInternal(root, newKey, rawPageNum0, rawPageNum1);
                                } catch (ItemInNextPageException itemInNextPageException) {
                                    itemInNextPageException.printStackTrace();
                                }

                            }
                            isInsertInternal = true;
                            isInsert = true;
                            continue;
                        }
                    } else {
                        stackNeed = true;
                    }

                    try {
                        insertInternalItem(parent, getMaxKey(rawPage), splitPageNum, rawPageNum, newKey);
                        isInsertInternal = true;
                        isInsert = true;

                    } catch (PageFullException ee) {
                        long rawPageNum0 = this.getRawPageNum();
                        Page rawPage0 = this.pageStorage.get(rawPageNum0);
                        newKey = insertFullInternal(parent, rawPage0, rawPageNum0, getMaxKey(rawPage), splitPageNum, rawPageNum, newKey);
                        rawPageNum = rawPageNum0;
                        rawPage = rawPage0;
                        splitPageNum = parentNum;
                    }  catch (ItemInNextPageException ee) {
                        parent = this.pageStorage.get(ee.getNextPageNum());
                        stackNeed = false;
                    }

                }
            }

        }
    }

    @Override
    synchronized public List<Long> query(long keyHash) {
        Page currentPage = this.root;
        long nextPageNum = 0;
        while (getPageType(currentPage) != PageType.LEAF) {
            try {
                nextPageNum = queryFirstInternalItem(currentPage, keyHash);
            } catch (ItemInNextPageException e) {
                currentPage = this.pageStorage.get(e.getNextPageNum());
                continue;
            }
            currentPage = this.pageStorage.get(nextPageNum);
        }
        int pos = 0;
        while (true) {
            try {
                pos = queryKeyPos(currentPage, keyHash);
                break;
            } catch (ItemInNextPageException e) {
                currentPage = this.pageStorage.get(e.getNextPageNum());
            }
        }
        List<Long> res = new LinkedList<>();
        long val = 0;
        while (true) {
            if (pos >= getPageItemNum(currentPage)) {
                pos = 0;
                currentPage = this.pageStorage.get(getPageNextPage(currentPage));
            }
            long key = getKeyByPosition(currentPage, pos);
            if (key == keyHash) {
                val = getValByPosition(currentPage, pos);
                res.add(val);
            } else {
                break;
            }
            pos ++;
        }
        return res;
    }

    @Override
    synchronized public Iterator<Pair> findFirst(long dataHash) {
        Page currentPage = this.root;
        long nextPageNum = 0;
        while (getPageType(currentPage) != PageType.LEAF) {
            try {
                nextPageNum = queryFirstInternalItem(currentPage, dataHash);
            } catch (ItemInNextPageException e) {
                currentPage = this.pageStorage.get(e.getNextPageNum());
                continue;
            }
            currentPage = this.pageStorage.get(nextPageNum);
        }
        int position = 0;
        while (true) {
            try {
                position = queryKeyPos(currentPage, dataHash);
                break;
            } catch (ItemInNextPageException e) {
                currentPage = this.pageStorage.get(e.getNextPageNum());
            }
        }
        return new BPlusTreeIterator(this,currentPage, position);
    }

    @Override
    synchronized public Iterator<Pair> findUpperbound(long dataHash) {
        Page currentPage = this.root;
        long nextPageNum = 0;
        while (getPageType(currentPage) != PageType.LEAF) {
            try {
                nextPageNum = queryUpperboundInternal(currentPage, dataHash);
            } catch (ItemInNextPageException e) {
                currentPage = this.pageStorage.get(e.getNextPageNum());
                continue;
            }
            currentPage = this.pageStorage.get(nextPageNum);
        }
        int position = 0;
        while (true) {
            try {
                position = queryUpperboundKeyPos(currentPage, dataHash);
                break;
            } catch (ItemInNextPageException e) {
                currentPage = this.pageStorage.get(e.getNextPageNum());
            }
        }
        return new BPlusTreeIterator(this,currentPage, position);
    }

    @Override
    synchronized public Iterator<Pair> findFirst() {
        Page currentPage = this.root;
        long nextPageNum = 0;
        while (getPageType(currentPage) != PageType.LEAF) {
            nextPageNum = getMinChild(currentPage);
            currentPage = this.pageStorage.get(nextPageNum);
        }
        return new BPlusTreeIterator(this,currentPage, 0);
    }

    /**
     * 用于Internal节点找到最小的儿子，服务于findFirst()
     * @param page 儿子的页号
     * @return
     */
    private long getMinChild(Page page) {
        return ByteUtil.bytes2Long(ByteUtil.subByte(page.getDataBytes(),BPlusTreeIndex.PAGE_BEGIN_POSTION + 8,8));
    }

    /**
     * LEAF节点的指定key的最前面的位置，服务于findFirst(dataHash)
     * @return key的位置
     */
    private int queryKeyPos(Page page,  long key) throws ItemInNextPageException {
        if ( key > getMaxKey(page) ) {
            throw new ItemInNextPageException(1, getPageNextPage(page));
        }
        int itemNum = getPageItemNum(page);
        int i = 0,j = itemNum;
        int temp = (i + j)/2;
        while (i < j) {
            if (getKeyByPosition(page, temp) > key){
                j = temp;
            }else if (getKeyByPosition(page, temp) < key){
                i = temp + 1;
            }else {
                break;
            }
            temp = (i + j ) / 2;
        }
        while (temp > 0 && getKeyByPosition(page, temp-1) >= key){
            temp--;
        }
       return temp;
    }

    /**
     * LEAF节点的指定大于key的最前面的位置，服务于findUpperbound(dataHash)
     * @param page
     * @param key
     * @return
     */
    private int queryUpperboundKeyPos(Page page, long key) throws ItemInNextPageException {
        if ( key >= getMaxKey(page) ) {
            throw new ItemInNextPageException(1, getPageNextPage(page));
        }
        int itemNum = getPageItemNum(page);
        int i = 0,j = itemNum;
        int temp = (i + j)/2;
        while (i < j) {
            if (getKeyByPosition(page, temp) > key){
                j = temp;
            }else if (getKeyByPosition(page, temp) < key){
                i = temp + 1;
            }else {
                break;
            }
            temp = (i + j ) / 2;
        }
        while (temp < itemNum - 1){
            if (getKeyByPosition(page, temp) > key) {
                break;
            }
            temp++;
        }
        return temp;
    }

    /**
     * 获取一个新页，并元页总页数自动加一
     * @return 新页的页号
     * @throws PageTypeException
     */
    private long getRawPageNum() {
        Page page = this.pageStorage.get(0);
        if (getPageType(page) != PageType.META) {
            try {
                throw new PageTypeException(2);
            } catch (PageTypeException e) {
                e.printStackTrace();
            }
        }
        long res = ByteUtil.bytes2Long(ByteUtil.subByte(page.getDataBytes(),4,8));
        byte[] bs = ByteUtil.long2Bytes(res + 1);
        try {
            page.patchData(4,bs);
        } catch (PageException e) {
            e.printStackTrace();
        }
        return res;
    }

    /**
     * 将根，从LEAF节点变为INTERNAL节点
     * @param page
     * @param minKey 通过minKey初始化根节点
     * @param minPageNum 小于minKey的页的页号
     * @param maxPageNum 大于minKey的页的页号
     */
    private void initRootInternal(Page page, long minKey, long minPageNum, long maxPageNum) {
        setPageType(page, PageType.INTERNAL);
        setPageItemNum(page, 2);
        setMaxKey(page, Long.MAX_VALUE);
        byte[] inserted;
        try {
            inserted = ByteUtil.byteMerger(ByteUtil.long2Bytes(minKey), ByteUtil.long2Bytes(minPageNum));
            page.patchData(BPlusTreeIndex.PAGE_BEGIN_POSTION,inserted);
            inserted = ByteUtil.byteMerger(ByteUtil.long2Bytes(Long.MAX_VALUE), ByteUtil.long2Bytes(maxPageNum));
            page.patchData(BPlusTreeIndex.PAGE_BEGIN_POSTION + 16,inserted);
        } catch (PageException e) {
            e.printStackTrace();
        }
    }

    /**
     * 将一个页初始化为根，且为LEAF节点
     * @param page
     */
    private void initRootLeaf(Page page) {
        setPageType(page, PageType.LEAF);
        setPageItemNum(page, 0);
        setMaxKey(page, Long.MAX_VALUE);
        setPageNextPage(page, 0);
    }

    /**
     *将internal节点的一个key（如果存在重复的key，指第一个）指向的页面替换成新的子页，同时在该条目前插入新的key.主要服务于于分裂操作
     * 如果满了或要插入的值
     * <pre>
     * oldKey - oldPageNum
     * >>>
     * newKey - oldPageNum
     * oldKey - replacePageNum
     * <pre/>
     * @param page
     * @param oldKey 待替换的key
     * @param replacePageNum 替换的新子页号
     * @param newKey 新插入的key
     * @param oldPageNum 待替换的页号
     * @return
     */
    private void insertInternalItem(Page page, long oldKey, long oldPageNum, long replacePageNum, long newKey) throws PageFullException, ItemInNextPageException {
        if ( oldKey > getMaxKey(page) ) {
            throw new ItemInNextPageException(1, getPageNextPage(page));
        }
        int itemNum = getPageItemNum(page);
        if ( itemNum >= BPlusTreeIndex.MAX_PAGE_ITEM ) {
            throw new PageFullException(1);
        }
        int i = 0,j = itemNum;
        int temp = (i + j)/2;
        while (i < j) {
            if (getKeyByPosition(page, temp) > oldKey){
                j = temp;
            }else if (getKeyByPosition(page, temp) < oldKey){
                i = temp + 1;
            }else {
                break;
            }
            temp = (i + j ) / 2;
        }
        int low = temp, high = temp + 1;
        boolean isFind = false;
        while (getKeyByPosition(page, low) == oldKey && low >= 0){
            if(getValByPosition(page, low) == oldPageNum) {
                temp = low;
                isFind = true;
                break;
            }
            low--;
        }
        while (getKeyByPosition(page, high) == oldKey && !isFind && high <= (BPlusTreeIndex.MAX_PAGE_ITEM - 1)) {
            if(getValByPosition(page, high) == oldPageNum) {
                temp = high;
                isFind = true;
                break;
            }
            high++;
        }
        if (!isFind) {
            throw new ItemInNextPageException(1,getPageNextPage(page));
        }

        try {
            byte[] bytes = ByteUtil.long2Bytes(replacePageNum);
            page.patchData(BPlusTreeIndex.PAGE_BEGIN_POSTION + temp * 16 + 8, bytes);
            byte[] bs = new byte[16];
            for (int k = itemNum; k > temp; k--) {
                System.arraycopy(page.getDataBytes(), BPlusTreeIndex.PAGE_BEGIN_POSTION + (k - 1) * 16 , bs, 0, 16);
                page.patchData(BPlusTreeIndex.PAGE_BEGIN_POSTION + k * 16 ,bs);
            }
            byte[] inserted = ByteUtil.byteMerger(ByteUtil.long2Bytes(newKey), ByteUtil.long2Bytes(oldPageNum));
            page.patchData(BPlusTreeIndex.PAGE_BEGIN_POSTION + temp * 16,inserted);
        } catch (PageException e) {
            e.printStackTrace();
        }
        setPageItemNum(page, itemNum + 1);
    }

    /**
     *将一个满的internal节点的一个key（如果存在重复的key，指第一个）指向的页面替换成新的子页，同时在该条目前插入新的key.主要服务于于分裂操作
     * <pre>
     * oldKey - oldPageNum
     * >>>
     * newKey - oldPageNum
     * oldKey - replacePageNum
     * <pre/>
     * @param page
     * @param newPage
     * @param newPageNum 新页的页号
     * @param oldKey 带替换的key
     * @param oldPageNum 待替换的页号
     * @param replacePageNum 替换的新子页号
     * @param newKey 新插入的key
     * @return 为分裂后新的key
     */
    private long insertFullInternal(Page page,  Page newPage, long newPageNum, long oldKey, long oldPageNum, long replacePageNum, long newKey) {
        if ( oldKey > getMaxKey(page)) {
            try {
                throw new ItemInNextPageException(1, getPageNextPage(page));
            } catch (ItemInNextPageException e) {
                e.printStackTrace();
            }
        }
        setPageType(newPage, PageType.INTERNAL);
        setMaxKey(newPage, getMaxKey(page));
        setPageNextPage(newPage, getPageNextPage(page));
        setPageNextPage(page, newPageNum);
        int i = 0,j = BPlusTreeIndex.MAX_PAGE_ITEM;
        int temp = (i + j)/2;
        while (i < j) {
            if (getKeyByPosition(page, temp) > oldKey){
                j = temp;
            }else if (getKeyByPosition(page, temp) < oldKey){
                i = temp + 1;
            }else {
                break;
            }
            temp = (i + j ) / 2;
        }
        int low = temp, high = temp + 1;
        boolean isFind = false;
        while (getKeyByPosition(page, low) == oldKey && low >= 0){
            if(getValByPosition(page, low) == oldPageNum) {
                temp = low;
                isFind = true;
                break;
            }
            low--;
        }
        while (getKeyByPosition(page, high) == oldKey && !isFind && high <= (BPlusTreeIndex.MAX_PAGE_ITEM - 1)) {
            if(getValByPosition(page, high) == oldPageNum) {
                temp = high;
                isFind = true;
                break;
            }
            high++;
        }
        if (!isFind) {
            try {
                throw new ItemInNextPageException(1, getPageNextPage(page));
            } catch (ItemInNextPageException e) {
                e.printStackTrace();
            }
        }
        //先对半分
        byte[] bs = new byte[16];
        try {
            for (int k = 0; k < BPlusTreeIndex.MAX_PAGE_ITEM/2; k++) {
                System.arraycopy(page.getDataBytes(), BPlusTreeIndex.PAGE_MID_POSTION + k * 16, bs, 0, 16);
                newPage.patchData(BPlusTreeIndex.PAGE_BEGIN_POSTION + k * 16,bs);
            }
        }
        catch (PageException e) {
            e.printStackTrace();
        }
        //在根据temp的取值情况分别处理
        if (temp < (BPlusTreeIndex.MAX_PAGE_ITEM)/2) {
            try {
                for (int k = BPlusTreeIndex.MAX_PAGE_ITEM/2; k > temp; k--) {
                    System.arraycopy(page.getDataBytes(), BPlusTreeIndex.PAGE_BEGIN_POSTION + (k - 1) * 16, bs, 0, 16);
                    page.patchData(BPlusTreeIndex.PAGE_BEGIN_POSTION + k * 16,bs);
                }
                page.patchData(BPlusTreeIndex.PAGE_BEGIN_POSTION + (temp + 1) * 16 + 8,ByteUtil.long2Bytes(replacePageNum));
                byte[] inserted = ByteUtil.byteMerger(ByteUtil.long2Bytes(newKey), ByteUtil.long2Bytes(oldPageNum));
                page.patchData(BPlusTreeIndex.PAGE_BEGIN_POSTION + temp * 16,inserted);
            } catch (PageException e) {
                e.printStackTrace();
            }
            setPageItemNum(page, BPlusTreeIndex.MAX_PAGE_ITEM/2 + 1);
            setPageItemNum(newPage,BPlusTreeIndex.MAX_PAGE_ITEM/2);
            long newKey0 = getKeyByPosition(page, BPlusTreeIndex.MAX_PAGE_ITEM/2);
            setMaxKey(page, newKey0);
            return newKey0;
        } else {
            temp = temp - BPlusTreeIndex.MAX_PAGE_ITEM/2;
            try {
                for (int k = BPlusTreeIndex.MAX_PAGE_ITEM/2; k > temp; k--) {
                    System.arraycopy(newPage.getDataBytes(), BPlusTreeIndex.PAGE_BEGIN_POSTION + (k - 1) * 16, bs, 0, 16);
                    newPage.patchData(BPlusTreeIndex.PAGE_BEGIN_POSTION + k * 16,bs);
                }
                newPage.patchData(BPlusTreeIndex.PAGE_BEGIN_POSTION + (temp + 1) * 16 + 8,ByteUtil.long2Bytes(replacePageNum));
                byte[] inserted = ByteUtil.byteMerger(ByteUtil.long2Bytes(newKey), ByteUtil.long2Bytes(oldPageNum));
                newPage.patchData(BPlusTreeIndex.PAGE_BEGIN_POSTION + temp * 16,inserted);
            } catch (PageException e) {
                e.printStackTrace();
            }
            setPageItemNum(page, BPlusTreeIndex.MAX_PAGE_ITEM/2);
            setPageItemNum(newPage,BPlusTreeIndex.MAX_PAGE_ITEM/2 + 1);
            long newKey0 = getKeyByPosition(page, BPlusTreeIndex.MAX_PAGE_ITEM/2 - 1);
            setMaxKey(page, newKey0);
            return newKey0;
        }
    }

    /**
     * 在internal节点中找到第一个keyHash有关的下一个节点，如果不满maxKey检验，则表明搜索的条目不再此页，抛出异常
     * @param page
     * @param keyHash
     * @return 相应儿子节点页号
     */
    private long queryFirstInternalItem(Page page,  long keyHash) throws ItemInNextPageException {
        if ( keyHash > getMaxKey(page) ) {
            throw  new ItemInNextPageException(1, getPageNextPage(page));
        }
        int itemNum = getPageItemNum(page);
        int i = 0,j = itemNum;
        int temp = (i + j)/2;
//        try {
            while (i < j ) {
                if (getKeyByPosition(page, temp) > keyHash){
                    j = temp;
                }else if (getKeyByPosition(page, temp) < keyHash){
                    i = temp + 1;
                }else {
                    break;
                }
                temp = (i + j ) / 2;
            }
//        }catch (ArrayIndexOutOfBoundsException e) {
//            System.out.println("werwe");
//        }
        while (temp >= 1 && getKeyByPosition(page, temp-1) >= keyHash){
            temp--;
        }
        return getValByPosition(page, temp);
    }

    /**
     * 在internal节点中找到大于keyHash有关的下一个节点，如果不满maxKey检验，则表明搜索的条目不再此页，抛出异常
     * @param page
     * @param keyHash
     * @return 相应儿子节点页号
     */
    private long queryUpperboundInternal(Page page,  long keyHash) throws ItemInNextPageException {
        if ( keyHash > getMaxKey(page) ) {
            throw  new ItemInNextPageException(1, getPageNextPage(page));
        }
        int itemNum = getPageItemNum(page);
        int i = 0,j = itemNum;
        int temp = (i + j)/2;
        while (i < j ) {
            if (getKeyByPosition(page, temp) > keyHash){
                j = temp;
            }else if (getKeyByPosition(page, temp) < keyHash){
                i = temp + 1;
            }else {
                break;
            }
            temp = (i + j ) / 2;
        }
        while (temp < itemNum && getKeyByPosition(page, temp) <= keyHash){
            temp++;
        }
        return getValByPosition(page, temp);
    }

    /**
     * 对树叶节点插入item,相应会抛出页满，条目不在相应页的异常
     * @param page
     * @param keyHash
     * @param uuid
     * @return
     */
    private void insertLeafItem(Page page, long keyHash, long uuid) throws ItemInNextPageException, PageFullException {
        if ( keyHash > getMaxKey(page)) {
            throw new ItemInNextPageException(1, getPageNextPage(page));
        }
        int itemNum = getPageItemNum(page);
        if ( itemNum >= BPlusTreeIndex.MAX_PAGE_ITEM ) {
            throw new PageFullException(1);
        }
        int i = 0,j = itemNum;
        int temp = (i + j)/2;
        while (i < j ) {
            long cmp = getKeyByPosition(page, temp);
//            long cmp0 = getKeyByPosition(page, temp - 1);
//            long cmp1 = getKeyByPosition(page, temp - 2);
            if (cmp > keyHash){
                j = temp;
            }else if (cmp < keyHash){
                i = temp + 1;
            }else {
                break;
            }
            temp = (i + j ) / 2;
        }
        try {
            byte[] bs = new byte[16];
            for (int k = itemNum; k > temp; k--) {
                System.arraycopy(page.getDataBytes(), BPlusTreeIndex.PAGE_BEGIN_POSTION + (k - 1) * 16, bs, 0, 16);
                page.patchData(BPlusTreeIndex.PAGE_BEGIN_POSTION + k * 16,bs);
            }
            byte[] inserted = ByteUtil.byteMerger(ByteUtil.long2Bytes(keyHash), ByteUtil.long2Bytes(uuid));
            page.patchData(BPlusTreeIndex.PAGE_BEGIN_POSTION + temp * 16,inserted);
        } catch (PageException e) {
            e.printStackTrace();
        }
        setPageItemNum(page, itemNum + 1);
    }

    /**
     *对一个已经满了的叶插入item
     * @param page 待分裂已满的页
     * @param newPage 一个未用过的新的页，用于分裂
     * @param newPageNum 新页的页号
     * @param keyHash 分裂时顺便插入的条目的键
     * @param uuid 分裂时顺便插入的条目的值
     * @return 为分裂后新的key
     */
    private long insertFullLeaf(Page page, Page newPage, long newPageNum, long keyHash, long uuid) {
        if ( keyHash > getMaxKey(page)) {
            try {
                throw new ItemInNextPageException(1, getPageNextPage(page));
            } catch (ItemInNextPageException e) {
                e.printStackTrace();
            }
        }
        setPageType(newPage, PageType.LEAF);
//        setPageItemNum(newPage,(BPlusTreeIndex.MAX_PAGE_ITEM + 1)/2);
        setMaxKey(newPage, getMaxKey(page));
        setPageNextPage(newPage, getPageNextPage(page));
        setPageNextPage(page, newPageNum);
        int i = 0,j = BPlusTreeIndex.MAX_PAGE_ITEM;
        int temp = (i + j)/2;
        while (i < j) {
            if (getKeyByPosition(page, temp) > keyHash){
                j = temp;
            }else if (getKeyByPosition(page, temp) < keyHash){
                i = temp + 1;
            }else {
                break;
            }
            temp = (i + j ) / 2;
        }
        if (temp < (BPlusTreeIndex.MAX_PAGE_ITEM)/2) {
            byte[] bs = new byte[16];
            try {
                for (int k = 0; k < BPlusTreeIndex.MAX_PAGE_ITEM/2; k++) {
                    System.arraycopy(page.getDataBytes(), BPlusTreeIndex.PAGE_MID_POSTION + k * 16, bs, 0, 16);
                    newPage.patchData(BPlusTreeIndex.PAGE_BEGIN_POSTION + k * 16, bs);
                }
            }
            catch (PageException e) {
                e.printStackTrace();
            }
            try {
                for (int k = BPlusTreeIndex.MAX_PAGE_ITEM/2; k > temp; k--) {
                    System.arraycopy(page.getDataBytes(), BPlusTreeIndex.PAGE_BEGIN_POSTION + (k - 1) * 16, bs, 0, 16);
                    page.patchData(BPlusTreeIndex.PAGE_BEGIN_POSTION + k * 16,bs);
                }
                byte[] inserted = ByteUtil.byteMerger(ByteUtil.long2Bytes(keyHash), ByteUtil.long2Bytes(uuid));
                page.patchData(BPlusTreeIndex.PAGE_BEGIN_POSTION + temp * 16,inserted);
            } catch (PageException e) {
                e.printStackTrace();
            }
            setPageItemNum(newPage,BPlusTreeIndex.MAX_PAGE_ITEM/2);
            setPageItemNum(page, BPlusTreeIndex.MAX_PAGE_ITEM/2 + 1);
            long newKey = ByteUtil.bytes2Long(ByteUtil.subByte(page.getDataBytes(), BPlusTreeIndex.PAGE_MID_POSTION,8));
            setMaxKey(page, newKey);
            return newKey;
        } else {
            byte[] bs = new byte[16];
            try {
                boolean isInsert = false;
                for (int k = 0; k < BPlusTreeIndex.MAX_PAGE_ITEM/2; k++) {
                    System.arraycopy(page.getDataBytes(), BPlusTreeIndex.PAGE_MID_POSTION + k * 16, bs, 0, 16);
                    if (keyHash < ByteUtil.bytes2Long(ByteUtil.subByte(bs, 0, 8)) && !isInsert) {
                        byte[] inserted = ByteUtil.byteMerger(ByteUtil.long2Bytes(keyHash), ByteUtil.long2Bytes(uuid));
                        newPage.patchData(BPlusTreeIndex.PAGE_BEGIN_POSTION + k * 16,inserted);
                        isInsert = true;
                    }
                    if (isInsert) {
                        newPage.patchData(BPlusTreeIndex.PAGE_BEGIN_POSTION + ( k + 1 ) * 16,bs);
                    } else {
                        newPage.patchData(BPlusTreeIndex.PAGE_BEGIN_POSTION + k * 16,bs);
                    }
                }
                if (!isInsert) {
                    byte[] inserted = ByteUtil.byteMerger(ByteUtil.long2Bytes(keyHash), ByteUtil.long2Bytes(uuid));
                    newPage.patchData(BPlusTreeIndex.PAGE_BEGIN_POSTION + BPlusTreeIndex.MAX_PAGE_ITEM/2 * 16,inserted);
                }
            }
            catch (PageException e) {
                e.printStackTrace();
            }
            setPageItemNum(newPage,BPlusTreeIndex.MAX_PAGE_ITEM/2 + 1);
            setPageItemNum(page, BPlusTreeIndex.MAX_PAGE_ITEM/2);
            long newKey = ByteUtil.bytes2Long(ByteUtil.subByte(page.getDataBytes(),BPlusTreeIndex.PAGE_MID_POSTION - 16,8));
            setMaxKey(page, newKey);
            return newKey;
        }
    }

    /**
     * 通过位置获得key
     * @param page
     * @param position
     * @return
     */
    private long getKeyByPosition(Page page, int position) {
        int pos = BPlusTreeIndex.PAGE_BEGIN_POSTION + 16*position;
        return ByteUtil.bytes2Long(ByteUtil.subByte(page.getDataBytes(),pos,8));
    }

    private long getValByPosition(Page page, int position) {
        int pos = BPlusTreeIndex.PAGE_BEGIN_POSTION + 16*position + 8;
        return ByteUtil.bytes2Long(ByteUtil.subByte(page.getDataBytes(),pos,8));
    }

    /**
     * 设置页的最大key字段
     * @param page
     * @param maxKey
     */
    private void setMaxKey(Page page, long maxKey) {
        byte[] bs = ByteUtil.long2Bytes(maxKey);
        try {
            page.patchData(16,bs);
        } catch (PageException e) {
            e.printStackTrace();
        }
    }

    /**
     * 读取页的最大key字段
     * @param page
     * @return
     */
    private long getMaxKey(Page page) {
        return ByteUtil.bytes2Long(ByteUtil.subByte(page.getDataBytes(),16,8));
    }

    /**
     * 设置某页的项目数量
     * @param page
     * @param itemNum
     */
    private void setPageItemNum(Page page, int itemNum) {
        byte[] bs = ByteUtil.int2Bytes(itemNum);
        try {
            page.patchData(4,bs);
        } catch (PageException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获得某页的项目数量
     * @param page
     * @return
     */
    private int getPageItemNum(Page page) {
        return ByteUtil.bytes2Int(ByteUtil.subByte(page.getDataBytes(),4,4));
    }

    /**
     * 设置某页的下一页的页码
     * @param page
     * @param pageNum
     */
    private void setPageNextPage(Page page, long pageNum) {
        byte[] bs = ByteUtil.long2Bytes(pageNum);
        try {
            page.patchData(8,bs);
        } catch (PageException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param page
     * @return 返回下一页的页码
     */
    private long getPageNextPage(Page page) {
        return ByteUtil.bytes2Long(ByteUtil.subByte(page.getDataBytes(),8,8));
    }

    /**
     *
     * @param page 页
     * @param pageType 设置页的类型
     */
    private void setPageType(Page page, PageType pageType) {
        int temp = pageType.ordinal();
        byte[] bs = ByteUtil.int2Bytes(temp);
        try {
            page.patchData(0,bs);
        } catch (PageException e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param page  页
     * @return 页的类型，如果不存在对应页类型，则抛出异常
     */
    public PageType getPageType(Page page) {
        int typeNum = ByteUtil.bytes2Int(ByteUtil.subByte(page.getDataBytes(),0,4));
        if (typeNum == PageType.INTERNAL.ordinal()) {
            return  PageType.INTERNAL;
        } else if (typeNum == PageType.LEAF.ordinal()) {
            return  PageType.LEAF;
        } else if (typeNum == PageType.META.ordinal()) {
            return PageType.META;
        }
        new PageTypeException(1);
        return null;
    }

    private class BPlusTreeIterator implements Iterator<Pair> {
        BPlusTreeIndex bPlusTreeIndex;
        Page currentPage;
        int currentPosition;
        int currentPageItemNum;

        public BPlusTreeIterator(BPlusTreeIndex bPlusTreeIndex, Page currentPage, int currentPosition) {
            this.bPlusTreeIndex = bPlusTreeIndex;
            this.currentPage = currentPage;
            this.currentPosition = currentPosition;
            this.currentPageItemNum = getPageItemNum(currentPage);
        }

        @Override
        public boolean hasNext() {
            synchronized (bPlusTreeIndex) {
                if (currentPageItemNum > currentPosition) {
                    return true;
                } else {
                    if (getPageNextPage(currentPage) != 0) {
                        return true;
                    }
                }
                return false;
            }
        }

        @Override
        public Pair next() {
            synchronized (bPlusTreeIndex) {
                if ( currentPageItemNum > currentPosition ) {
                    Pair res = new Pair(getKeyByPosition(currentPage,currentPosition), getValByPosition(currentPage,currentPosition));
                    currentPosition++;
                    return res;
                }
                else {
                    long nextPageNum = getPageNextPage(currentPage);
                    if (nextPageNum == 0) {
                        return null;
                    } else {
                        System.out.println("nextPage*****************");
                        currentPage = this.bPlusTreeIndex.pageStorage.get(nextPageNum);
                        currentPageItemNum = getPageItemNum(currentPage);
                        currentPosition = 0;
                        Pair res = new Pair(getKeyByPosition(currentPage,currentPosition), getValByPosition(currentPage,currentPosition));
                        currentPosition++;
                        return res;
                    }
                }
            }
        }
    }
}
