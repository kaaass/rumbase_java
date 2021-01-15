package net.kaaass.rumbase.index;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.index.exception.IndexAlreadyExistException;

import java.util.Random;

@Slf4j
public class ConcurrentIndexTest extends TestCase {
    public static final String fileDir = "build/";

    /**
     * 测试索引的并发功能
     */
    public void test() {
        Index testIndex = null;
        try {
            testIndex = Index.createEmptyIndex(fileDir + "test$id");
        } catch (IndexAlreadyExistException e) {
            log.error("Exception Error :", e);
        }

        Index finalTestIndex = testIndex;
        Thread thread = new Thread(() -> {
            for (int i = 40000; i >= 0; i--) {

                assert finalTestIndex != null;

                finalTestIndex.insert(i, new Random().nextLong());
            }
        });

        thread.start();

        for (int i = 40000; i >= 0; i--) {

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
            assertEquals(cnt / 2,
                    pair.getKey());
            //log.debug("{}", pair);
            cnt++;
        }
    }

    /**
     * 测试索引的更复杂的并发功能
     */
    public void testComplex() {
        Index testIndex = null;
        try {
            testIndex = Index.createEmptyIndex(fileDir + "test$id");
        } catch (IndexAlreadyExistException e) {
            log.error("Exception Error :", e);
        }

        Index finalTestIndex = testIndex;
        Thread thread1 = new Thread(() -> {
            for (int i = 40000; i >= 0; i--) {

                assert finalTestIndex != null;

                finalTestIndex.insert(i, new Random().nextLong());
            }
        });

        thread1.start();

        Thread thread2 = new Thread(() -> {
            for (int i = 0; i <= 40000; i++) {

                assert finalTestIndex != null;

                finalTestIndex.insert(i, new Random().nextLong());
            }
        });

        thread2.start();

        for (int i = 40000; i >= 0; i--) {

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
            assertEquals(cnt / 3,
                    pair.getKey());
            //log.debug("{}", pair);
            cnt++;
        }
    }
}
