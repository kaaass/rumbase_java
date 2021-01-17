package net.kaaass.rumbase.index;

import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.FileUtil;
import net.kaaass.rumbase.index.exception.IndexAlreadyExistException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Iterator;
import java.util.Random;

/**
 * 对B+树进行测试
 *
 * @author DoctorWei1314
 * @see net.kaaass.rumbase.index.BTreeTest
 */
@Slf4j
public class BTreeTest {
    public static final String fileDir = FileUtil.TEST_PATH;

    @BeforeClass
    public static void createDataFolder() {
        FileUtil.prepare();
    }

    @AfterClass
    public static void clearDataFolder() {
        FileUtil.clear();
    }

    /**
     * 测试索引的插入与第一个迭代器功能
     */
    @Test
    public void testInsert() {
        Index testIndex = null;
        try {
            new File(fileDir + "BtreetestInsert$id").deleteOnExit();
            testIndex = Index.createEmptyIndex(fileDir + "BtreetestInsert$id");
        } catch (IndexAlreadyExistException e) {
            log.error("Exception Error :", e);
        }

        for (int i = 4000; i >= 0; i--) {

            assert testIndex != null;
            testIndex.insert(i, new Random().nextLong());

            testIndex.insert(i, new Random().nextLong());
        }

        // 测试数据是否符合预期
        int cnt = 0;
        for (var pair : testIndex) {
            Assert.assertEquals(cnt / 2,
                    pair.getKey());
            // log.debug("{}", pair);
            cnt++;
        }
    }

    /**
     * 测试不按顺寻key插入的情况
     */
    @Test
    public void testInsertRandomKey() {
        Index testIndex = null;
        try {
            new File(fileDir + "testInsertRandomKey$id").deleteOnExit();
            testIndex = Index.createEmptyIndex(fileDir + "testInsertRandomKey$id");
        } catch (IndexAlreadyExistException e) {
            log.error("Exception Error :", e);
        }

        for (int i = 4000; i >= 0; i--) {

            assert testIndex != null;
            testIndex.insert(i, new Random().nextLong());

            testIndex.insert(4000 - i, new Random().nextLong());
        }

        // 测试数据是否符合预期
        int cnt = 0;
        for (var pair : testIndex) {
            Assert.assertEquals(cnt / 2,
                    pair.getKey());
            // log.debug("{}", pair);
            cnt++;
        }
    }

    /**
     * 测试索引的查询功能
     */
    @Test
    public void testQuery() {
        Index testIndex = null;

        try {
            new File(fileDir + "BtreetestQuery$id").deleteOnExit();
            testIndex = Index.createEmptyIndex(fileDir + "BtreetestQuery$id");
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
        Assert.assertTrue(it1.hasNext());
        Assert.assertEquals(3, it1.next().getKey());
        Assert.assertEquals(3, it1.next().getKey());
        Assert.assertEquals(4, it1.next().getKey());

        // 测试 findUpperbound 方法
        // 不包括keyHash在内的迭代器 112233->4455
        Iterator<Pair> it2 = testIndex.findUpperbound(3);
        Assert.assertTrue(it2.hasNext());
        Assert.assertEquals(4, it2.next().getKey());
        Assert.assertEquals(4, it2.next().getKey());
        Assert.assertEquals(5, it2.next().getKey());

        // 测试 query 方法
        var results = testIndex.query(4);
        System.out.println(results);
//        assertTrue(results.contains(standardRand.get(2)));
//        assertTrue(results.contains(standardRand.get(2 + 1)));
    }

    /**
     * 测试multiKey索引的查询功能
     */
    @Test
    public void testMultiKeyQuery() {
        Index testIndex = null;

        try {
            new File(fileDir + "testMultiKeyQuery$id").deleteOnExit();
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
        Assert.assertTrue(it1.hasNext());
        Assert.assertEquals(3, it1.next().getKey());
        Assert.assertEquals(3, it1.next().getKey());
        Assert.assertEquals(3, it1.next().getKey());

        // 测试 findUpperbound 方法
        // 不包括keyHash在内的迭代器 112233->4455
        Iterator<Pair> it2 = testIndex.findUpperbound(3);
        Assert.assertTrue(it2.hasNext());
        Assert.assertEquals(4, it2.next().getKey());
        Assert.assertEquals(4, it2.next().getKey());
        Assert.assertEquals(4, it2.next().getKey());
        for (int i = 0; i < 3; i++) {
            log.debug("{}", it2.next().getUuid());
        }

//        assertTrue(results.contains(standardRand.get(2)));
//        assertTrue(results.contains(standardRand.get(2 + 1)));
    }
}
