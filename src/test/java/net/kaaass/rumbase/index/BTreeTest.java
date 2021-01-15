package net.kaaass.rumbase.index;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.index.exception.IndexAlreadyExistException;
import net.kaaass.rumbase.index.exception.IndexNotFoundException;

import java.util.Iterator;
import java.util.Random;

/**
 * 对B+树进行测试
 *
 * @author DoctorWei1314
 * @see net.kaaass.rumbase.index.BTreeTest
 */
@Slf4j
public class BTreeTest extends TestCase {
    public static final String fileDir = "build/";

    /**
     * 测试索引对象管理与拿取
     */
    public void testIndexManagement() throws IndexAlreadyExistException, IndexNotFoundException {
        // 测试索引是否存在，表示student表的id字段索引，table_name$field_name
        assertFalse("don't exists such a index", Index.exists(fileDir + "student$id"));

        // 创建一个空索引，如果已经存在，则抛出异常
        Index.createEmptyIndex(fileDir + "student$id");
        try {
//            Index.createEmptyIndex("student$name");
            Index.createEmptyIndex(fileDir + "student$score");
            Index.createEmptyIndex(fileDir + "student$score");
            fail("should get exception");
        } catch (IndexAlreadyExistException e) {
            log.error("Exception Error :", e);
        }

        // 拿到这个索引,若没有则抛出异常
        Index.getIndex(fileDir + "student$id");
        Index.getIndex(fileDir + "employee$id");
    }

    /**
     * 测试索引的插入与第一个迭代器功能
     */
    public void testInsert() {
        Index testIndex = null;
        try {
            testIndex = Index.createEmptyIndex(fileDir + "testInsert$id");
        } catch (IndexAlreadyExistException e) {
            log.error("Exception Error :", e);
        }

        for (int i = 40000; i >= 0; i--) {

            assert testIndex != null;
            testIndex.insert(i, new Random().nextLong());

            testIndex.insert(i, new Random().nextLong());
        }

        // 测试数据是否符合预期
        int cnt = 0;
        for (var pair : testIndex) {
            assertEquals(cnt / 2,
                    pair.getKey());
            log.debug("{}", pair);
            cnt++;
        }
    }

    /**
     * 测试索引的查询功能
     */
    public void testQuery() {
        Index testIndex = null;

        try {
            testIndex = Index.createEmptyIndex(fileDir + "testQuery$id");
        } catch (IndexAlreadyExistException e) {
            log.error("Exception Error :", e);
        }

        // 倒序添加若干随机数据
        for (int i = 2000; i > 0; i--) {
            var rand = new Random().nextLong();

            assert testIndex != null;
            testIndex.insert(i, rand);

            testIndex.insert(i, new Random().nextLong());
        }

        // 打印当前索引情况
        for (var pair : testIndex) {
            log.debug("{}", pair);
        }

        // 测试 findFirst 方法
        // keyHash在内的迭代器 1122->334455
        Iterator<Pair> it1 = testIndex.findFirst(3);
        assertTrue(it1.hasNext());
        assertEquals(3, it1.next().getKey());
        assertEquals(3, it1.next().getKey());
        assertEquals(4, it1.next().getKey());

        // 测试 findUpperbound 方法
        // 不包括keyHash在内的迭代器 112233->4455
        Iterator<Pair> it2 = testIndex.findUpperbound(3);
        assertTrue(it2.hasNext());
        assertEquals(4, it2.next().getKey());
        assertEquals(4, it2.next().getKey());
        assertEquals(5, it2.next().getKey());

        // 测试 query 方法
        var results = testIndex.query(4);
        System.out.println(results);
//        assertTrue(results.contains(standardRand.get(2)));
//        assertTrue(results.contains(standardRand.get(2 + 1)));
    }

    /**
     * 测试multiKey索引的查询功能
     */
    public void testMultiKeyQuery() {
        Index testIndex = null;

        try {
            testIndex = Index.createEmptyIndex(fileDir + "testMultiKeyQuery$id");
        } catch (IndexAlreadyExistException e) {
            log.error("Exception Error :", e);
        }

        // 倒序添加若干随机数据
        for (int i = 30; i > 0; i--) {
            assert testIndex != null;
            for (int j = 0; j < 50; j++) {
                testIndex.insert(i, new Random().nextLong());
                testIndex.insert(i, new Random().nextLong());
            }
        }

        int y = 0;
        // 打印当前索引情况
        for (var pair : testIndex) {
            log.debug("{}{}", y % 100, pair);
            y++;
        }

        // 测试 query 方法
        var results = testIndex.query(4);
        System.out.println(results);

        // 测试 findFirst 方法
        // keyHash在内的迭代器 1122->334455
        Iterator<Pair> it1 = testIndex.findFirst(3);
        assertTrue(it1.hasNext());
        assertEquals(3, it1.next().getKey());
        assertEquals(3, it1.next().getKey());
        assertEquals(3, it1.next().getKey());

        // 测试 findUpperbound 方法
        // 不包括keyHash在内的迭代器 112233->4455
        Iterator<Pair> it2 = testIndex.findUpperbound(3);
        assertTrue(it2.hasNext());
        assertEquals(4, it2.next().getKey());
        assertEquals(4, it2.next().getKey());
        assertEquals(4, it2.next().getKey());
        for (int i = 0; i < 3; i++) {
            log.debug("{}", it2.next().getUuid());
        }

//        assertTrue(results.contains(standardRand.get(2)));
//        assertTrue(results.contains(standardRand.get(2 + 1)));
    }
}
