package net.kaaass.rumbase.table.field;

import com.igormaznitsa.jbbp.io.JBBPBitInputStream;
import com.igormaznitsa.jbbp.io.JBBPBitNumber;
import com.igormaznitsa.jbbp.io.JBBPByteOrder;
import lombok.*;
import net.kaaass.rumbase.index.Index;
import net.kaaass.rumbase.index.Pair;
import net.kaaass.rumbase.index.exception.IndexAlreadyExistException;
import net.kaaass.rumbase.table.Table;
import net.kaaass.rumbase.table.exception.TableConflictException;
import net.kaaass.rumbase.table.exception.TableExistenceException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;


/**
 * 字段结构
 * <p>
 * 提供字段解析服务
 * <p>
 * 提供字段的索引处理
 *
 * @author @KveinAxel
 */
@RequiredArgsConstructor
public abstract class BaseField{

    /**
     * 字段名
     */
    @Getter
    @NonNull
    protected final String name;

    /**
     * 字段类型
     */
    @Getter
    @NonNull
    private final FieldType type;

    /**
     * 字段是否可空
     */
    @Getter
    private final boolean nullable;

    /**
     * 索引
     * <p>
     * 如果未建立索引则为空
     */
    protected Index index = null;

    /**
     * 索引名
     */
    @Getter
    @Setter
    protected String indexName;

    /**
     * 当前列所属的表
     */
    @NonNull
    @Getter
    @Setter
    private Table parentTable;

    /**
     * 向输出流中写入当前字段格式信息
     * <p>
     * 格式为：
     * <p>
     *     列名
     * <p>
     *     列类型
     * <p>
     *     是否可空
     * <p>
     *     是否建立索引
     * <p>
     *     索引名(如果建立索引)
     * <p>
     *     列参数(可选)
     * <p>
     *
     * @param stream 输出流
     */
    public abstract void persist(OutputStream stream);

    /**
     * 从输入流中读入当前字段格式信息，并构造、返回当前字段
     *
     * @param stream 输入流
     * @return 字段
     */
    public static BaseField load(InputStream stream, Table table) {
        var in = new JBBPBitInputStream(stream);

        try {
            var name = in.readString(JBBPByteOrder.BIG_ENDIAN);
            var type = FieldType.valueOf(in.readString(JBBPByteOrder.BIG_ENDIAN));
            var flag = in.readByte();
            var nullable = (flag & 1) == 1;
            var indexed = (flag & 2) == 2;
            String indexName = null;
            if (indexed) {
                indexName = in.readString(JBBPByteOrder.BIG_ENDIAN);
            }
            // todo 重建索引

            BaseField field;
            switch (type) {
                case INT:
                    field = new IntField(name, nullable, table);
                    field.setIndexName(indexName);
                    return field;
                case FLOAT:
                    field = new FloatField(name, nullable, table);
                    field.setIndexName(indexName);
                    return field;
                default:
                    field = new VarcharField(name, in.readInt(JBBPByteOrder.BIG_ENDIAN), nullable, table);
                    field.setIndexName(indexName);
                    return field;
            }
        } catch (IOException e) {
            // todo
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 判断字符串是否能够转成符合当前字段约束的值
     * @param valStr 待检查字符串
     * @return 满足情况
     */
    public abstract boolean checkStr(String valStr);

    /**
     * 将字符串转成哈希
     * @param str 待转换字符串
     * @throws TableConflictException 字段类型不匹配
     * @return 哈希
     */
    public abstract long strToHash(String str) throws TableConflictException;


    /**
     * 将值对象转成哈希
     * @param val 值对象
     * @throws TableConflictException 字段类型不匹配
     * @return 哈希
     */
    public abstract long toHash(Object val) throws TableConflictException;

    /**
     * 从输入流中反序列化出一个满足当前字段约束的值对象
     * @param inputStream 输入流
     * @throws TableConflictException 输入流中读出对象与字段类型不匹配
     * @return 值对象
     */
    public abstract Object deserialize(InputStream inputStream) throws TableConflictException;

    /**
     * 从输入流中反序列化一个满足当前类型的对象，并判断是否满足约束
     * @param inputStream 输入流
     * @return 满足情况
     */
    public abstract boolean checkInputStream(InputStream inputStream);

    /**
     * 将值对象序列化到输出流中
     * <p>
     * 先序列化一位isNull，1 -> null ; 0 -> 非null
     * <p>
     * 如果非null则继续序列化他的值
     *
     * @param outputStream 输出流
     * @param strVal 值对象
     * @throws TableConflictException 类型不匹配
     */
    public abstract void serialize(OutputStream outputStream, String strVal) throws TableConflictException;


    /**
     * 将值对象序列化到输出流中
     * <p>
     * 序列化方法同上
     *
     * @param outputStream 输出流
     * @param val 值对象
     * @throws TableConflictException 类型不匹配
     */
    public abstract void serialize(OutputStream outputStream, Object val) throws TableConflictException;



    /**
     * 创建索引
     *
     * @throws IndexAlreadyExistException 索引已存在
     */
    public void createIndex() throws IndexAlreadyExistException {
        var delimiter = "$";
        var indexName = parentTable.getTableName() + delimiter + name;

        index = Index.createEmptyIndex(indexName);
    }

    /**
     * 向索引插入一个键值对
     * @param value 值对象
     * @param uuid uuid
     * @throws TableConflictException 字段类型不匹配
     * @throws TableExistenceException 索引不存在
     */
    public abstract void insertIndex(String value, long uuid) throws TableConflictException, TableExistenceException;


    /**
     * 向索引插入一个键值对
     * @param value 值对象
     * @param uuid uuid
     * @throws TableConflictException 字段类型不匹配
     * @throws TableExistenceException 索引不存在
     */
    public abstract void insertIndex(Object value, long uuid) throws TableConflictException, TableExistenceException;



    /**
     * 通过当前字段的索引树查询记录
     *
     * @param key 字段值
     * @throws TableConflictException 字段类型不匹配
     * @throws TableExistenceException 索引不存在
     * @return 记录的uuid
     */
    public abstract List<Long> queryIndex(String key) throws TableExistenceException, TableConflictException;

    /**
     * 通过当前字段的索引树查询记录
     *
     * @param key 字段值
     * @throws TableConflictException 字段类型不匹配
     * @throws TableExistenceException 索引不存在
     * @return 记录的uuid
     */
    public abstract List<Long> queryIndex(Object key) throws TableExistenceException, TableConflictException;

    /**
     * 查询当前索引的第一个迭代器
     *
     * @throws TableExistenceException 索引不存在
     * @return 迭代器
     */
    public abstract Iterator<Pair> queryFirst() throws TableExistenceException;

    /**
     * 查询第一个满足字段值的迭代器
     *
     * @param key 查询键
     * @throws TableExistenceException 索引不存在
     * @throws TableConflictException 字段类型不匹配
     * @return 迭代器
     */
    public abstract Iterator<Pair> queryFirstMeet(String key) throws TableExistenceException, TableConflictException;

    /**
     * 查询第一个满足值且与查询键不相等的迭代器
     *
     * @param key 查询键
     * @throws TableExistenceException 索引不存在
     * @throws TableConflictException 字段类型不匹配
     * @return 迭代器
     */
    public abstract Iterator<Pair> queryFirstMeetNotEqual(String key) throws TableExistenceException, TableConflictException;

    /**
     * 是否建立索引
     * @return true -> 已建立索引; false -> 未建立索引
     */
    public boolean indexed() {
        return index != null;
    }

    /**
     * 将str转成值
     *
     * @param str 字符串
     * @return 值
     * @throws TableConflictException 字段类型不匹配
     */
    public abstract Object strToValue(String str) throws TableConflictException;

    /**
     * 检测值是否满足约束
     *
     * @param val 值
     * @return true -> 满足约束； false -> 不满足约束
     */
    public abstract boolean checkObject(Object val);

    /**
     * 比较两个字段大小
     * @param a 第一个字段
     * @param b 第二个字段
     * @return 比较结果
     * @throws TableConflictException 字段类型不匹配
     */
    public abstract int compare(Object a, Object b) throws TableConflictException;
}