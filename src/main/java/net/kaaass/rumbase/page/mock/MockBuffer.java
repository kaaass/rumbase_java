package net.kaaass.rumbase.page.mock;

import net.kaaass.rumbase.page.PageManager;
import net.kaaass.rumbase.page.exception.BufferExeception;

import java.util.concurrent.locks.ReentrantLock;

public class MockBuffer {
    private static MockBuffer instance = null;
    private int size = 0;

    private MockBuffer() {
        this.lock = new ReentrantLock();
        size = PageManager.PAGE_NUM;
    }

    public static MockBuffer getInstance() {
        if (instance == null) {
            synchronized (MockBuffer.class) {
                if (instance == null) {
                    instance = new MockBuffer();
                }
            }
        }
        return instance;
    }

    private final byte[] byteBuffer = new byte[PageManager.BYTE_BUFFER_SIZE];

    public void put(int offset, byte[] bytes) throws BufferExeception {
        if (this.size <= 0) {
            throw new BufferExeception(1);
        }
        lock.lock();
        try {
            System.arraycopy(bytes, 0, this.byteBuffer, offset, bytes.length);
            this.size--;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    public byte[] get(int offset) {
        lock.lock();
        try {
            byte[] temp = new byte[PageManager.PAGE_SIZE];
            System.arraycopy(this.byteBuffer, offset, temp, offset, PageManager.PAGE_SIZE);
            return temp;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
        return null;
    }

    public void free(int offset) {
        lock.lock();
        try {
            System.arraycopy(this.byteBuffer, offset, new byte[PageManager.PAGE_SIZE], 0, PageManager.PAGE_SIZE);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    private ReentrantLock lock = null;
}
