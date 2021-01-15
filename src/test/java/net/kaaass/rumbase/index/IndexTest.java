package net.kaaass.rumbase.index;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.index.exception.IndexAlreadyExistException;
import net.kaaass.rumbase.index.exception.IndexNotFoundException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Random;

/**
 * 对索引部分进行测试
 *
 * @author DoctorWei1314
 * @see net.kaaass.rumbase.index.Index
 */
@Slf4j
public class IndexTest extends TestCase {
    public static final String fileDir = "build/";

    /**
     * 测试索引的插入与第一个迭代器功能
     */
    public void testInsert() {
        Index testIndex = null;
        var standardRand = new ArrayList<Long>();
        try {
            testIndex = Index.createEmptyIndex(fileDir + "testInsert$id");
        } catch (IndexAlreadyExistException e) {
            log.error("Exception Error :", e);
        }

        for (int i = 0; i < 50; i++) {
            var rand = new Random().nextLong();

            standardRand.add(rand);
            assert testIndex != null;
            testIndex.insert(i, rand);
        }

        // 测试数据是否符合预期
        int cnt = 0;
        for (var pair : testIndex) {
            assertEquals(standardRand.get(cnt).longValue(),
                    pair.getUuid());
            cnt++;
        }
    }

    /**
     * 测试索引的查询功能
     */
    public void testQuery() {
        Index testIndex = null;
        var standardRand = new ArrayList<Long>();

        try {
            testIndex = Index.createEmptyIndex(fileDir + "testQuery$id");
        } catch (IndexAlreadyExistException e) {
            log.error("Exception Error :", e);
        }

        // 倒序添加若干随机数据
        for (int i = 5; i > 0; i--) {
            var rand = new Random().nextLong();

            standardRand.add(rand);
            assert testIndex != null;
            testIndex.insert(i, rand);

            standardRand.add(rand = new Random().nextLong());
            testIndex.insert(i, rand);
        }

        // 打印当前索引情况
        for (var pair : testIndex) {
            log.debug("{}", pair);
        }

        // 测试 findFirst 方法
        // keyHash在内的迭代器 1122->334455
        Iterator<Pair> it1 = testIndex.findFirst(3);
        assertTrue(it1.hasNext());
        var expected = List.of(
                standardRand.get(2 * 2),
                standardRand.get(2 * 2 + 1)
        );
        assertTrue(expected.contains(it1.next().getUuid()));
        assertTrue(expected.contains(it1.next().getUuid()));

        // 测试 findUpperbound 方法
        // 不包括keyHash在内的迭代器 112233->4455
        Iterator<Pair> it2 = testIndex.findUpperbound(3);
        assertTrue(it2.hasNext());
        expected = List.of(
                standardRand.get(2),
                standardRand.get(2 + 1)
        );
        assertTrue(expected.contains(it2.next().getUuid()));
        assertTrue(expected.contains(it2.next().getUuid()));

        // 测试 query 方法
        var results = testIndex.query(4);
        assertTrue(results.contains(standardRand.get(2)));
        assertTrue(results.contains(standardRand.get(2 + 1)));
    }
}
