package net.kaaass.rumbase.page.mock;

import net.kaaass.rumbase.page.Page;
import net.kaaass.rumbase.page.PageManager;
import net.kaaass.rumbase.page.exception.FileExeception;
import net.kaaass.rumbase.page.exception.PageException;

import java.io.*;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class MockPage implements Page {
    private byte[] data;
    long pageId;
    boolean dirty;
    int pinned = 0;
    String filepath;
    private final ReentrantLock lock;

    MockPage(byte[] data, long pageId, String filepath) {
        this.data = data;
        this.pageId = pageId;
        this.lock = new ReentrantLock();
        this.dirty = false;
        this.filepath = filepath;
    }

    @Override
    public byte[] getData() {
        synchronized(this){
            pin();
            return data;
        }
    }

    @Override
    public void writeData(byte[] data) {
        lock.lock();
        this.data = data;
        this.dirty = true;
        lock.unlock();
        unpin();
    }

    @Override
    public void patchData(int offset, byte[] data) throws PageException {
        if (offset + data.length > PageManager.PAGE_SIZE) {
            throw new PageException(1);
        }
        System.arraycopy(data, 0, this.data, offset, data.length);
    }

    @Override
    public void flush() throws FileExeception {
        File file = new File(this.filepath);
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
