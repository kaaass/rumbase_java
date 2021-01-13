package net.kaaass.rumbase.table.field;

import com.igormaznitsa.jbbp.io.JBBPBitInputStream;
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
public abstract class BaseField {

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

    protected Index index = null;

    @NonNull
    @Getter
    private final Table parentTable;

    /**
     * 向输出流中写入当前字段格式信息
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
            switch (type) {
                case INT:
                    return new IntField(name, table);
                case FLOAT:
                    return new FloatField(name, table);
                default:
                    return new VarcharField(name, in.readInt(JBBPByteOrder.BIG_ENDIAN), table);
            }
        } catch (IOException e) {
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
     * @param outputStream 输出流
     * @param strVal 值对象
     * @throws TableConflictException 类型不匹配
     */
    public abstract void serialize(OutputStream outputStream, String strVal) throws TableConflictException;

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
     * 通过当前字段的索引树查询记录
     *
     * @param key 字段值
     * @throws TableConflictException 字段类型不匹配
     * @throws TableExistenceException 索引不存在
     * @return 记录的uuid
     */
    public abstract List<Long> queryIndex(String key) throws TableExistenceException, TableConflictException;

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

    public boolean indexed() {
        return index != null;
    }

}