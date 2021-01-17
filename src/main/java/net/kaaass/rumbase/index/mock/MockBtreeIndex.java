package net.kaaass.rumbase.index.mock;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import net.kaaass.rumbase.index.Index;
import net.kaaass.rumbase.index.Pair;

import java.util.*;

/**
 * @author 无索魏
 */
@RequiredArgsConstructor
public class MockBtreeIndex implements Index {
    /**
     * 内存中创建的MockBtreeIndex
     */
    public static Map<String, MockBtreeIndex> MOCK_BTREE_INDEX_MAP = new HashMap<>();

    @Getter
    private final HashMap<Long, List<Long>> hashMap = new HashMap<>();

    @Override
    public void replace(Map<Long, Long> uuidMap) {
        var values = hashMap.values();
        for (List<Long> value : values) {
            for (Long old : value) {
                if (uuidMap.containsKey(old)) {
                    value.remove(old);
                    value.add(uuidMap.get(old));
                }
            }
        }
    }

    @Override
    public void insert(long dataHash, long uuid) {
        hashMap.computeIfAbsent(dataHash, k -> new ArrayList<>()).add(uuid);
    }

    @Override
    public List<Long> query(long dataHash) {
        return hashMap.get(dataHash) == null ? new LinkedList<>() : hashMap.get(dataHash);
    }

    @Override
    public Iterator<Pair> findFirst(long keyHash) {
        return new MockIterator(this, keyHash, true);
    }

    @Override
    public Iterator<Pair> findUpperbound(long keyHash) {
        return new MockIterator(this, keyHash, false);
    }

    @Override
    public Iterator<Pair> findFirst() {
        return new MockIterator(this);
    }
}
