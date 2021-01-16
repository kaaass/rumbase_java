package net.kaaass.rumbase.page;

import net.kaaass.rumbase.page.exception.FileException;
import net.kaaass.rumbase.page.exception.PageException;

import java.io.File;
import java.io.RandomAccessFile;

/**
 * Page实现
 * <p>
 * 页持有的是整个缓冲区的指针和偏移。页在patchData时本身并不加锁，如果防止冲突需要在上层加锁。
 * </p>
 * @author XuanLaoYee
 */
public class RumPage implements Page {
    public RumPage(byte[] data, long pageId, String filepath, int offset) {
        this.data = data;
        this.pageId = pageId;
        this.dirty = false;
        this.filepath = filepath;
        this.offset = offset;//在内存中的偏移,指的是以页为单位的偏移
        try{
            out = new RandomAccessFile(filepath, "rw");
        }catch(Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 得到的是数据的副本，而非缓冲区的指针。
     * @return clone后的数据
     */
    @Override
    public byte[] getDataBytes() {
        synchronized (this) {
            byte[] tmp = new byte[PageManager.PAGE_SIZE];
            System.arraycopy(this.data, this.offset * PageManager.PAGE_SIZE, tmp, 0, tmp.length);
            return tmp;
        }
    }

    /**
     *
     * @param offset 页内偏移值，以字节为单位 ，该过程不加锁
     * @param data   待写入数据
     * @throws PageException 回写数据偏移与大小之和超过规定，则抛出异常
     */
    @Override
    public void patchData(int offset, byte[] data) throws PageException {
        if (offset + data.length > PageManager.PAGE_SIZE) {
            throw new PageException(1);
        }
        synchronized (this) {
            this.dirty = true;
        }
        //直接往缓冲内写入
        System.arraycopy(data, 0, this.data, this.offset * PageManager.PAGE_SIZE + offset, data.length);
    }

    /**
     * 还未实现double write
     * @throws FileException
     */
    @Override
    public void flush() throws FileException {
        File file = new File(this.filepath);
        synchronized (this) {
            try {
                try {
                    out.seek((PageManager.FILE_HEAD_SIZE + this.pageId) * (long)PageManager.PAGE_SIZE);
                } catch (Exception e) {
                    throw new FileException(4);
                }
                try {
                    byte[] data = new byte[PageManager.PAGE_SIZE];
                    System.arraycopy(this.data,this.offset*PageManager.PAGE_SIZE,data,0,data.length);
                    out.write(data);
                } catch (Exception e) {
                    throw new FileException(2);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
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

    @Override
    public boolean equals(Object obj) {
        return this == obj;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    public boolean pinned() {
        return pinned > 0;
    }

    public boolean dirty() {
        return dirty;
    }

    public Long pageId(){
        return this.pageId;
    }

    @Override
    protected void finalize() {
        try{
            this.out.close();
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
    private final byte[] data;
    private final long pageId;
    boolean dirty;
    int pinned = 0;
    String filepath;
    int offset;
    RandomAccessFile out = null;
}
