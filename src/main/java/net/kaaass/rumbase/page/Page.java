package net.kaaass.rumbase.page;

import net.kaaass.rumbase.page.exception.FileException;
import net.kaaass.rumbase.page.exception.PageException;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

/**
 * 单页管理
 * <p>
 * Page对象通过PageCache获得。获得的对象【不可以长期保存】（如保存在对象中作为对象的字段）。
 * 使用页面需要注意使用pin，以防止Page对象被提前回收。
 * <pre>
 *     var page = pageCache.get(1);
 *     page.pin();
 *     // ...
 *     page.getData();
 *     // ...
 *     // 如果不pin，则此处page对象可能因为被淘汰失效
 *     page.patchData(2, patch);
 *     page.unpin();
 * <pre/>
 * 如果操作有错误风险（有可能发生不造成停机的错误），必须加上try-catch-finally
 * <pre>
 *     var page = pageCache.get(1);
 *     page.pin();
 *     try {
 *         page.getData();
 *         // ...
 *         page.patchData(2, patch);
 *     } catch (Exception e) {
 *         // ...
 *     } finally {
 *         page.unpin(); // 重要：防止内存泄漏，必须在finally释放
 *     }
 * <pre/>
 * 若操作数量为1，不复用Page对象的情形，可以忽略。
 * <pre>
 *     pageCache.get(1).getData();
 * <pre/>
 *
 * @author XuanLaoYee
 */
public interface Page {

    /**
     * （性能原因，建议使用getData）获得页内数据
     *
     * @return 字节数组，通常为4K
     */
    byte[] getDataBytes();

    /**
     * 获得页内数据的字节流
     * <p>
     * 目前使用default方法实现，留作将来优化性能用
     *
     * @return 字节流
     */
    default InputStream getData() {
        return new ByteArrayInputStream(getDataBytes());
    }

    /**
     * 将字节数据写入页
     *
     * @param data 待写入字节数据
     * @throws PageException TODO
     */
    default void writeData(byte[] data) throws PageException {
        patchData(0, data);
    }

    /**
     * 在页指定位置写入数据
     *
     * @param offset 页内偏移值，以字节为单位
     * @param data   待写入数据
     * @throws PageException TODO
     */
    void patchData(int offset, byte[] data) throws PageException;

    void flush() throws FileException;

    /**
     * 将页固定在内存中
     * <p>
     * 操作规约：必须在系列操作之前使用pin，以防止页被回收
     */
    void pin();

    /**
     * 将页从内存中取消固定
     * <p>
     * 操作规约：必须在系列操作之后使用unpin，以防止内存泄露
     */
    void unpin();
}
