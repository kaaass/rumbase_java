package net.kaaass.rumbase.index;

import net.kaaass.rumbase.index.exception.IndexAlreadyExistException;
import net.kaaass.rumbase.index.exception.IndexNotFoundException;
import net.kaaass.rumbase.index.mock.MockBtreeIndex;
import net.kaaass.rumbase.record.exception.RecordNotFoundException;

import java.nio.file.LinkOption;
import java.security.KeyPair;
import java.util.*;

/**
 * 索引
 *
 * @author DoctorWei1314
 */

public interface Index {
    /**
     * 通过文件名拿到索引,如果不存在，则抛出异常
     *
     * @param indexFileName
     * @return
     */
    static Index getIndex(String indexFileName) throws IndexNotFoundException {
        if (MockBtreeIndex.MOCK_BTREE_INDEX_MAP.get(indexFileName) == null){
            throw new IndexNotFoundException(1);
        }
        return MockBtreeIndex.MOCK_BTREE_INDEX_MAP.get(indexFileName);
    }

    /**
     * 查看索引文件是否存在
     *
     * @param indexFileName
     * @return
     */
    static boolean exists(String indexFileName) {
        return MockBtreeIndex.MOCK_BTREE_INDEX_MAP.get(indexFileName) != null;
    }

    /**
     * 创建一个空的索引
     *
     * @param indexFileName
     * @return
     */
    static Index createEmptyIndex(String indexFileName) throws IndexAlreadyExistException {
        if (MockBtreeIndex.MOCK_BTREE_INDEX_MAP.get(indexFileName) == null){
            MockBtreeIndex.MOCK_BTREE_INDEX_MAP.put(indexFileName,new MockBtreeIndex());
        }
        else throw new IndexAlreadyExistException(1);
        return MockBtreeIndex.MOCK_BTREE_INDEX_MAP.get(indexFileName);
    }

    /**
     *替换uuid
     *
     * @param uuidMap
     */
    void replace(Map<Long, Long> uuidMap);

    /**
     * 插入
     *
     * @param dataHash
     * @param uuid
     */
    void insert(long dataHash, long uuid);

    /**
     * 返回对应key的uuid，如果不存在，则list为空
     * @param keyHash
     * @return
     */
    List<Long> query(long keyHash);

    /**
     * 返回包括keyHash在内的迭代器 eg: keyHash = 4,   33^^^445577
     * @param keyHash
     * @return
     */
    Iterator<Pair> queryWith(long keyHash);

    /**
     * 返回不包括keyHash在内的迭代器 eg: keyHash = 4,   3344^^^5577
     * @param keyHask
     * @return
     */
    Iterator<Pair> queryWithout(long keyHask);

    /**
     * 返回第一个版本的迭代器
     * @return
     */
    Iterator<Pair> getFirst();
}
