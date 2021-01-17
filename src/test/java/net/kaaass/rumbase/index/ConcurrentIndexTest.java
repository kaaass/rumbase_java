package net.kaaass.rumbase.index;

import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.FileUtil;
import net.kaaass.rumbase.index.exception.IndexAlreadyExistException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Random;

@Slf4j
public class ConcurrentIndexTest {
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
     * 测试索引的并发功能
     */
    @Test
    public void test() {
        Index testIndex = null;
        try {
            new File(fileDir + "test$id").deleteOnExit();
            testIndex = Index.createEmptyIndex(fileDir + "test$id");
        } catch (IndexAlreadyExistException e) {
            log.error("Exception Error :", e);
        }

        Index finalTestIndex = testIndex;
        Thread thread = new Thread(() -> {
            for (int i = 4000; i >= 0; i--) {

                assert finalTestIndex != null;

                finalTestIndex.insert(i, new Random().nextLong());
            }
        });

        thread.start();

        for (int i = 4000; i >= 0; i--) {

            assert testIndex != null;
            testIndex.insert(i, new Random().nextLong());
        }

        try {
            thread.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // 测试数据是否符合预期
        int cnt = 0;
        for (var pair : testIndex) {
            Assert.assertEquals(cnt / 2,
                    pair.getKey());
            //log.debug("{}", pair);
            cnt++;
        }
    }

    /**
     * 测试索引的更复杂的并发功能
     */
    @Test
    public void testComplex() {
        Index testIndex = null;
        try {
            new File(fileDir + "ConcurrenttestComplex$id").deleteOnExit();
            testIndex = Index.createEmptyIndex(fileDir + "ConcurrenttestComplex$id");
        } catch (IndexAlreadyExistException e) {
            log.error("Exception Error :", e);
        }

        Index finalTestIndex = testIndex;
        Thread thread1 = new Thread(() -> {
            for (int i = 4000; i >= 0; i--) {

                assert finalTestIndex != null;

                finalTestIndex.insert(i, new Random().nextLong());
            }
        });

        thread1.start();

        Thread thread2 = new Thread(() -> {
            for (int i = 0; i <= 4000; i++) {

                assert finalTestIndex != null;

                finalTestIndex.insert(i, new Random().nextLong());
            }
        });

        thread2.start();

        for (int i = 4000; i >= 0; i--) {

            assert testIndex != null;
            testIndex.insert(i, new Random().nextLong());
        }

        try {
            thread1.join();
            thread2.join();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        // 测试数据是否符合预期
        int cnt = 0;
        for (var pair : testIndex) {
            Assert.assertEquals(cnt / 3,
                    pair.getKey());
            //log.debug("{}", pair);
            cnt++;
        }
    }
}
