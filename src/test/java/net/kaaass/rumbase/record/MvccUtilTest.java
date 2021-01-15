package net.kaaass.rumbase.record;

import junit.framework.TestCase;
import lombok.extern.slf4j.Slf4j;

import java.util.Random;

@Slf4j
public class MvccUtilTest extends TestCase {

    public void testReadWriteInt() {
        byte[] arr = new byte[5];
        for (int i = 0; i < Integer.MAX_VALUE - 100; i += 50) {
            MvccUtil.writeInt(arr, 1, i);
            assertEquals(i, MvccUtil.readInt(arr, 1));
        }
    }

    public void testReadWriteLong() {
        byte[] arr = new byte[13];
        var rand = new Random();
        for (int i = 0; i < 10000; i++) {
            long num = rand.nextLong();
            MvccUtil.writeLong(arr, 4, num);
            assertEquals(num, MvccUtil.readLong(arr, 4));
        }
    }
}