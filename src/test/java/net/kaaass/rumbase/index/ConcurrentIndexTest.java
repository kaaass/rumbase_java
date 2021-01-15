package net.kaaass.rumbase.index;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;
import net.kaaass.rumbase.index.exception.IndexAlreadyExistException;

import java.util.Random;

@Slf4j
public class ConcurrentIndexTest extends TestCase {
    /**
     * 测试索引的并发功能
     */
    public void test() {
        Index testIndex = null;
        try {
            testIndex = Index.createEmptyIndex("test$id");
        } catch (IndexAlreadyExistException e) {
            log.error("Exception Error :", e);
        }

        Index finalTestIndex = testIndex;
        Thread thread = new Thread(new Runnable() {
            @Override
            public void run() {
                for (int i = 40000; i >= 0; i--) {

                    assert finalTestIndex != null;

                    finalTestIndex.insert(i, new Random().nextLong());
                }
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
            assertEquals(cnt/2,
                    pair.getKey());
            log.debug("{}", pair);
            cnt++;
        }
    }
}
