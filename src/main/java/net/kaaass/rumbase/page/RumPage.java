package net.kaaass.rumbase.page;

import net.kaaass.rumbase.page.exception.FileException;
import net.kaaass.rumbase.page.exception.PageException;

import java.io.File;
import java.io.RandomAccessFile;

/**
 * @author XuanLaoYee
 */
public class RumPage implements Page {
    public RumPage(byte[] data, long pageId, String filepath, int offset) {
        this.data = data;
        this.pageId = pageId;
        this.dirty = false;
        this.filepath = filepath;
        this.offset = offset;//在内存中的偏移,指的是以页为单位的偏移
    }

    @Override
    public byte[] getDataBytes() {
        synchronized (this) {
            byte[] tmp = new byte[PageManager.PAGE_SIZE];
            System.arraycopy(this.data, this.offset * PageManager.PAGE_SIZE, tmp, 0, tmp.length);
            return tmp;
        }
    }

    @Override
    public void patchData(int offset, byte[] data) throws PageException {
        if (offset + data.length > PageManager.PAGE_SIZE) {
            throw new PageException(1);
        }
        //直接往缓冲内写入
        System.arraycopy(data, 0, this.data, this.offset * PageManager.PAGE_SIZE + offset, data.length);
    }

    @Override
    public void flush() throws FileException {
        File file = new File(this.filepath);
        synchronized (this) {
            try {
                RandomAccessFile out = new RandomAccessFile(file, "rw");
                try {
                    out.seek((PageManager.FILE_HEAD_SIZE + this.pageId) * PageManager.PAGE_SIZE);
                } catch (Exception e) {
                    throw new FileException(4);
                }
                try {
                    out.write(data);
                } catch (Exception e) {
                    throw new FileException(2);
                }
                out.close();
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

    private final byte[] data;
    private final long pageId;
    boolean dirty;
    int pinned = 0;
    String filepath;
    int offset;
}
