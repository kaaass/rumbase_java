package net.kaaass.rumbase.page;

import net.kaaass.rumbase.page.exception.FileExeception;
import net.kaaass.rumbase.page.exception.PageException;

import java.io.FileNotFoundException;

/**
 * 单页管理
 * <p>
 * Page对象通过PageCache获得
 * </p>
 *
 * @author @XuanLaoYee
 */
public interface Page {

    public byte[] getData();

    public void writeData(byte[] data);

    /**
     * 在页指定位置写入数据
     *
     * @param offset 页内偏移值，以字节为单位
     * @param data 待写入数据
     */
    public void patchData(int offset, byte[] data) throws PageException;

    public void flush() throws FileExeception;

    /**
     * 将页钉在内存中
     *
     * <p>
     * 执行查询或插入之前需要调用
     * </p>
     */
    public void pin();

    /**
     * 将页从内存中取消固定
     *
     * <p>
     * 执行完指定查询或插入之后需要调用，
     * </p>pin和unpin需要成对出现
     */
    public void unpin();

}
