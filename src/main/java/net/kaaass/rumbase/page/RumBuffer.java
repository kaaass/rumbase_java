package net.kaaass.rumbase.page;

import net.kaaass.rumbase.page.exception.BufferException;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 内存管理
 * <p>
 * 使用单例模式，使用byte数组开辟连续空间，内存的大小不得超过BYTE_BUFFER_SIZE，RumBuffer在操作时直接将整块内存锁住
 * </p>
 * @author XuanLaoYee
 */
public class RumBuffer {
    private static RumBuffer instance = null;
    private int size = 0;
    private ReentrantLock lock = null;
    private final byte[] byteBuffer = new byte[PageManager.BYTE_BUFFER_SIZE];
    private List<Integer> freePage = null;

    private RumBuffer(){
        this.lock = new ReentrantLock();
        size = PageManager.PAGE_NUM;
        this.size = PageManager.BUFFER_SIZE;
        this.freePage = new ArrayList<>();
        for(int i=0;i<this.size;i++){
            this.freePage.add(i);
        }
    }

    public static RumBuffer getInstance() {
        if (instance == null) {
            synchronized (RumBuffer.class) {
                if (instance == null) {
                    instance = new RumBuffer();
                }
            }
        }
        return instance;
    }

    /**
     * 将页大小的byte数组写入至缓冲的对应偏移中
     * @param offset 这里的偏移指的是按照页的大小进行偏移，比如3代表缓存数组中第3*PAGE_SIZE的位置
     * @param bytes 这里的数组大小等于PAGE_SIZE
     * @throws BufferException 若缓冲大小为0，则抛出内存不足的异常。若占用已装入内存空间，则抛出占用非空内存位置异常
     */
    public void put(int offset, byte[] bytes) throws BufferException {
        if (this.size <= 0) {
            throw new BufferException(1);
        }
        lock.lock();
        try {
            System.arraycopy(bytes, 0, this.byteBuffer, offset*PageManager.PAGE_SIZE, bytes.length);
            if(!this.freePage.contains(offset)){
                throw new BufferException(3);
            }
            this.freePage.remove((Integer)offset);
            this.size--;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
    }

    /**
     * 从内存中读取指定位置的长度为页长的数据
     * @param offset 这里的偏移指的是按照页的大小进行偏移
     * @return 返回大小为页长的数组，该数组占用的内存与缓冲区无关
     */
    public byte[] get(int offset) {
        lock.lock();
        try {
            byte[] temp = new byte[PageManager.PAGE_SIZE];
            System.arraycopy(this.byteBuffer, offset*PageManager.PAGE_SIZE, temp, offset, PageManager.PAGE_SIZE);
            return temp;
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            lock.unlock();
        }
        return null;
    }

    /**
     * 释放缓冲区偏移为offset * page_size 处，大小为页长的空间
     * @param offset 这里的偏移指的是按照页的大小进行偏移
     */
    public void free(int offset) {
        synchronized(RumBuffer.getInstance()){
            lock.lock();
            try {
                System.arraycopy(this.byteBuffer, offset*PageManager.PAGE_SIZE, new byte[PageManager.PAGE_SIZE], 0, PageManager.PAGE_SIZE);
                this.freePage.add(offset);
                this.size++;
            } catch (Exception e) {
                e.printStackTrace();
            } finally {
                lock.unlock();
            }
        }

    }

    /**
     * 返回缓冲区指针
     * @return 缓冲区指针
     */
    public byte[] buffer(){
        return byteBuffer;
    }

    /**
     * 返回空闲页的偏移
     * @return 空闲页的偏移
     */
    public int getFreeOffset() {
        synchronized (this){
            return this.freePage.get(0);
        }
    }
}
