package net.kaaass.rumbase.page.mock;

import net.kaaass.rumbase.page.Page;
import net.kaaass.rumbase.page.PageManager;
import net.kaaass.rumbase.page.exception.FileExeception;
import net.kaaass.rumbase.page.exception.PageException;

import java.io.*;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MockPage implements Page {
    private byte[] data;
    long pageId = -1;
    boolean dirty = false;
    int pinned = 0;
    String filepath = null;
    private ReentrantReadWriteLock lock = null;

    MockPage(byte[] data, long pageId, String filepath) {
        this.data = data;
        this.pageId = pageId;
        this.lock = new ReentrantReadWriteLock();
        this.dirty = false;
        this.filepath = filepath;
    }

    @Override
    public byte[] getData() { //?读这个地方怎么加锁
        synchronized(this){
            pin();
            return data;
        }
    }

    @Override
    public void writeData(byte[] data) {
        lock.writeLock().lock();
        pin();
        this.data = data;
        this.dirty = true;
        unpin();
        lock.writeLock().unlock();
    }

    @Override
    public void patchData(int offset, byte[] data) throws PageException {
        if (offset + data.length > PageManager.PAGE_SIZE) {
            throw new PageException(1);
        }
        lock.writeLock().lock();
        System.arraycopy(data, 0, this.data, offset, data.length);
        lock.writeLock().unlock();
    }

    @Override
    public void flush() throws FileExeception {
        File file = new File(this.filepath);
        OutputStream os = null;
        try {
            RandomAccessFile out = new RandomAccessFile(file, "rw");
            try{
                out.seek((PageManager.FILE_HEAD_SIZE + this.pageId) * PageManager.PAGE_SIZE);
            }catch(Exception e){
                throw new FileExeception(4);
            }
            try{
                out.write(data);
            }catch(Exception e){
                throw new FileExeception(2);
            }
            out.close();
        } catch (Exception e) {
            throw new FileExeception(3);
        }
    }

    @Override
    public void pin() {
        synchronized (this) {
            pinned++;
        }
    }

    @Override
    public void unpin() {
        synchronized (this) {
            pinned--;
        }
    }
}
