package net.kaaass.rumbase.table;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

/**
 * 字段结构
 * <p>
 * 提供字段解析服务
 * <p>
 * 提供字段的索引处理
 *
 * @author @KveinAxel
 */
public interface IField {

    /**
     * 从配置信息的字节数组获取字段的属性
     *
     * @param raw 配置信息
     */
    void parseSelf(byte[] raw);

    /**
     * 将自身的配置信息持久化成字节数组
     *
     * @return 配置信息
     */
    byte[] persistSelf();

    /**
     * 是否已经被建立引用
     * @return true则已经建立引用，反之没建立
     */
    Boolean isIndexed();

    /**
     * 获取字段的类型
     *
     * @return 字段类型
     */
    FieldType getType();

    /**
     * 字段所占用的字节数
     *
     * @return 字节数
     */
    int getSize();

    /**
     * 在该字段的索引上进行范围搜索
     *
     * @param left 查询左端点
     * @param right 查询右端点
     * @return 查询到的uuid
     */
    List<UUID> searchRange(FieldValue left, FieldValue right);

    /**
     * 在该字段的索引上进行单点搜索
     *
     * @param value 字段的值
     * @return 字段的uuid的Optional
     */
    Optional<UUID> search(FieldValue value);

    /**
     * 将字符串转换成字段值
     *
     * @param valStr 字符串
     * @return 字段值的Optional
     */
    Optional<FieldValue> strToValue(String valStr);

    /**
     * 将字段值转成字节数组
     *
     * @param value 字段值
     * @return 字节数组
     */
    Optional<byte[]> valueToRaw(FieldValue value);

    /**
     * 将字节数组转成字段值
     *
     * @param raw 字节数组
     * @return 字段值
     */
    Optional<FieldValue> rawToValue(byte[] raw) ;

    /**
     * 获取字段名
     *
     * @return 字段名
     */
    String getFieldName();

}
