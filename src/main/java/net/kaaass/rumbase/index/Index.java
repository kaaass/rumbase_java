package net.kaaass.rumbase.index;

import net.kaaass.rumbase.index.btree.BPlusTreeIndex;
import net.kaaass.rumbase.index.exception.IndexAlreadyExistException;
import net.kaaass.rumbase.index.exception.IndexNotFoundException;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * 索引
 *
 * @author DoctorWei1314
 */

public interface Index extends Iterable<Pair> {
    /**
     * 通过文件名拿到索引,如果不存在，则抛出异常
     *
     * @param indexFileName
     * @return
     * @throws IndexNotFoundException
     */
    static Index getIndex(String indexFileName) throws IndexNotFoundException {
        if (BPlusTreeIndex.B_PLUS_TREE_INDEX_MAP.get(indexFileName) != null) {
            return BPlusTreeIndex.B_PLUS_TREE_INDEX_MAP.get(indexFileName);
        } else {
            BPlusTreeIndex bPlusTreeIndex = new BPlusTreeIndex(indexFileName);
            if (!bPlusTreeIndex.isIndexedFile()) {
                throw new IndexNotFoundException(1);
            }
            BPlusTreeIndex.B_PLUS_TREE_INDEX_MAP.put(indexFileName, bPlusTreeIndex);
            return bPlusTreeIndex;
        }
    }

    /**
     * 查看索引文件是否存在
     *
     * @param indexFileName
     * @return
     */
    static boolean exists(String indexFileName) {
        return new File(indexFileName).exists();
    }

    /**
     * 创建一个空的索引
     *
     * @param indexFileName
     * @return
     * @throws IndexAlreadyExistException
     */
    static Index createEmptyIndex(String indexFileName) throws IndexAlreadyExistException {
        BPlusTreeIndex bPlusTreeIndex = new BPlusTreeIndex(indexFileName);
        if (bPlusTreeIndex.isIndexedFile()) {
            throw new IndexAlreadyExistException(1);
        }
        bPlusTreeIndex.initPage();
        BPlusTreeIndex.B_PLUS_TREE_INDEX_MAP.put(indexFileName, bPlusTreeIndex);
        return bPlusTreeIndex;
    }

    /**
     * 对索引节点的uuid值进行批量替换
     *
     * @param uuidMap UUID替换表，键为旧UUID，值为替换的目标UUID
     */
    void replace(Map<Long, Long> uuidMap);

    /**
     * 向索引中加入键值对“数据Hash-UUID”
     *
     * @param dataHash 数据Hash
     * @param uuid     对应记录UUID
     */
    void insert(long dataHash, long uuid);

    /**
     * 查询所有数据Hash对应的UUID
     *
     * @param dataHash 数据Hash
     * @return 返回所有对应的UUID，若不存在则为空
     */
    List<Long> query(long dataHash);

    /**
     * 查找第一个键为dataHash的键值对
     * <p>
     * 如 dataHash = 4，当前键值有：
     * <pre>
     * 3 3 [4 4] 5 5 7 7
     *      ↑迭代器指向
     * <pre/>
     *
     * @param dataHash 数据Hash
     * @return 指向该键值对的迭代器
     */
    Iterator<Pair> findFirst(long dataHash);

    /**
     * 查找键为dataHash的上界
     * <p>
     * 如 dataHash = 4，当前键值有：
     * <pre>
     * 3 3 [4 4] 5 5 7 7
     *           ↑迭代器指向
     * <pre/>
     *
     * @param dataHash 数据Hash
     * @return 指向该键值对的迭代器
     */
    Iterator<Pair> findUpperbound(long dataHash);

    /**
     * 返回第一个键值对的迭代器
     *
     * @return 指向该键值对的迭代器
     */
    Iterator<Pair> findFirst();

    /**
     * 返回遍历索引的迭代器
     *
     * @return 第一个键值对的迭代器
     */
    @Override
    default Iterator<Pair> iterator() {
        return findFirst();
    }
}
