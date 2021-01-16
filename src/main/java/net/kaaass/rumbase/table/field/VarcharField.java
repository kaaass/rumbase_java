package net.kaaass.rumbase.table.field;

import com.igormaznitsa.jbbp.io.JBBPBitInputStream;
import com.igormaznitsa.jbbp.io.JBBPBitOutputStream;
import com.igormaznitsa.jbbp.io.JBBPByteOrder;
import lombok.Getter;
import lombok.NonNull;
import net.kaaass.rumbase.index.Pair;
import net.kaaass.rumbase.table.Table;
import net.kaaass.rumbase.table.exception.TableConflictException;
import net.kaaass.rumbase.table.exception.TableExistenceException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;


/**
 * 可变长字符串类型的字段
 *
 * @author @KveinAxel
 */
public class VarcharField extends BaseField {

    /**
     * 字符串长度的限制
     */
    @Getter
    private final int limit;

    private static final String DELIMIT = "'";

    public VarcharField(@NonNull String name, int limit, boolean nullable, @NonNull Table parentTable) {
        super(name, FieldType.VARCHAR, nullable, parentTable);
        this.limit = limit;
    }

    @Override
    public void persist(OutputStream stream) {
        var out = new JBBPBitOutputStream(stream);

        try {
            out.writeString(getName(), JBBPByteOrder.BIG_ENDIAN);
            out.writeString(getType().toString().toUpperCase(Locale.ROOT), JBBPByteOrder.BIG_ENDIAN);
            var flags = new byte[]{0};
            flags[0] |= indexed() ? 1 : 0;
            if (indexed()) {
                flags[0] |= 2;
                out.writeBytes(flags, 1, JBBPByteOrder.BIG_ENDIAN);
                out.writeString(indexName, JBBPByteOrder.BIG_ENDIAN);
            } else {
                out.writeBytes(flags, 1, JBBPByteOrder.BIG_ENDIAN);
            }
            out.writeInt(limit, JBBPByteOrder.BIG_ENDIAN);
            // todo （字段约束）
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean checkStr(String valStr) {
        if (valStr == null || valStr.isBlank()) {
            return isNullable();
        }
        return valStr.startsWith(DELIMIT) && valStr.endsWith(DELIMIT) && valStr.length() <= this.limit + 2 * DELIMIT.length();
    }

    @Override
    public long strToHash(String str) {

        // 空值的哈希固定为0
        if (str == null || str.isBlank()) {
            return 0;
        }

        return str.hashCode();
    }

    @Override
    public long toHash(Object val) throws TableConflictException {

        // 空值的哈希固定为0
        if (val == null) {
            return 0;
        }

        try {
            var str = (String) val;
            return strToHash(str);
        } catch (ClassCastException e) {
            throw new TableConflictException(1);
        }
    }

    @Override
    public Object deserialize(InputStream inputStream) throws TableConflictException {

        var stream = new JBBPBitInputStream(inputStream);

        try {
            var flag = stream.readByte();
            var isNull = (flag & 1) == 1;
            if (isNull) {
                if (isNullable()) {
                    return null;
                } else {
                    throw new TableConflictException(3);
                }
            }

            return stream.readString(JBBPByteOrder.BIG_ENDIAN);
        } catch (IOException e) {
            throw new TableConflictException(1);
        }
    }

    @Override
    public boolean checkInputStream(InputStream inputStream) {

        var stream = new JBBPBitInputStream(inputStream);

        try {
            var flag = stream.readByte();
            var isNull = (flag & 1) == 1;
            if (isNull) {
                return isNullable();
            }

            var str = stream.readString(JBBPByteOrder.BIG_ENDIAN);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public void serialize(OutputStream outputStream, String strVal) throws TableConflictException {

        var stream = new JBBPBitOutputStream(outputStream);

        if (!checkStr(strVal)) {
            throw new TableConflictException(3);
        }

        try {
            if (strVal == null || strVal.isBlank()) {
                stream.writeBytes(new byte[]{1}, 1, JBBPByteOrder.BIG_ENDIAN);
            } else {
                if (strVal.startsWith(DELIMIT) && strVal.endsWith(DELIMIT)) {
                    var substr = strVal.substring(1, strVal.length() - 1);
                    if (substr.isBlank()) {
                        stream.writeBytes(new byte[]{1}, 1, JBBPByteOrder.BIG_ENDIAN);
                    } else {
                        stream.writeBytes(new byte[]{0}, 1, JBBPByteOrder.BIG_ENDIAN);
                        stream.writeString(substr, JBBPByteOrder.BIG_ENDIAN);
                    }
                } else {
                    throw new TableConflictException(1);
                }
            }

        } catch (IOException e) {
            // fixme 这个给外面可能也不知道如何处理
            throw new RuntimeException(e);
        }
    }

    @Override
    public void serialize(OutputStream outputStream, Object val) throws TableConflictException {

        var stream = new JBBPBitOutputStream(outputStream);

        if (!checkObject(val)) {
            throw new TableConflictException(3);
        }

        try {
            if (val == null) {
                stream.writeBytes(new byte[]{1}, 1, JBBPByteOrder.BIG_ENDIAN);
            } else {
                stream.writeBytes(new byte[]{0}, 1, JBBPByteOrder.BIG_ENDIAN);
                stream.writeString((String) val, JBBPByteOrder.BIG_ENDIAN);
            }

        } catch (IOException e) {
            // fixme 这个给外面可能也不知道如何处理
            throw new RuntimeException(e);
        } catch (ClassCastException e){
            throw new TableConflictException(1);
        }
    }

    @Override
    public void insertIndex(String value, long uuid) throws TableExistenceException {
        if (index == null) {
            throw new TableExistenceException(6);
        }
        index.insert(strToHash(value), uuid);
    }

    @Override
    public void insertIndex(Object value, long uuid) throws TableConflictException, TableExistenceException {
        if (index == null) {
            throw new TableExistenceException(6);
        }
        index.insert(toHash(value), uuid);
    }

    @Override
    public List<Long> queryIndex(String value) throws TableExistenceException {
        if (index == null) {
            throw new TableExistenceException(6);
        }

        return index.query(strToHash(value));
    }

    @Override
    public List<Long> queryIndex(Object key) throws TableExistenceException, TableConflictException {
        if (index == null) {
            throw new TableExistenceException(6);
        }

        try {
            return index.query(toHash(key));
        } catch (ClassCastException e) {
            throw new TableConflictException(1);
        }
    }

    @Override
    public Iterator<Pair> queryFirst() throws TableExistenceException {
        if (index == null) {
            throw new TableExistenceException(6);
        }

        return index.findFirst();
    }

    @Override
    public Iterator<Pair> queryFirstMeet(String key) throws TableExistenceException {
        if (index == null) {
            throw new TableExistenceException(6);
        }

        return index.findFirst(strToHash(key));
    }

    @Override
    public Iterator<Pair> queryFirstMeetNotEqual(String key) throws TableExistenceException {
        if (index == null) {
            throw new TableExistenceException(6);
        }

        return index.findUpperbound(strToHash(key));
    }

    @Override
    public Object strToValue(String str) {
        if (str.startsWith(DELIMIT) && str.endsWith(DELIMIT)) {
            return str.substring(1, str.length() - 1);
        } else {
            return str;
        }
    }

    @Override
    public boolean checkObject(Object val) {
        try {
            var res = (String) val;
            return true;
        } catch (ClassCastException e) {
            return false;
        }
    }

    @Override
    public int compare(Object a, Object b) throws TableConflictException {
        try {
            return ((String) a).compareTo((String) b);
        } catch (ClassCastException e) {
            throw new TableConflictException(1);
        }
    }

}
