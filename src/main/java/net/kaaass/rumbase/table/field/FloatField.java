package net.kaaass.rumbase.table.field;

import com.igormaznitsa.jbbp.io.JBBPBitInputStream;
import com.igormaznitsa.jbbp.io.JBBPBitOutputStream;
import com.igormaznitsa.jbbp.io.JBBPByteOrder;
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
 * 单浮点数类型的字段
 *
 * @author @KveinAxel
 */
public class FloatField extends BaseField {

    public FloatField(@NonNull String name, boolean nullable, @NonNull Table parentTable) {
        super(name, FieldType.FLOAT, nullable, parentTable);
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
        try {
            return checkVal(Float.parseFloat(valStr));
        } catch (NumberFormatException e) {
            return false;
        }
    }

    /**
     * 字段约束（待定）
     *
     * @param val 值
     * @return 是否满足约束
     */
    boolean checkVal(float val) {
        return true;
    }

    @Override
    public long strToHash(String str) throws TableConflictException {

        // 空值的哈希固定为0
        if (str == null || str.isBlank()) {
            return 0;
        }

        try {
            var val = Float.parseFloat(str);
            if (checkVal(val)) {
                return toHash(val);
            } else {
                throw new TableConflictException(3);
            }
        } catch (NumberFormatException e) {
            throw new TableConflictException(1);
        }
    }

    @Override
    public long toHash(Object val) throws TableConflictException {

        // 空值的哈希固定为0
        if (val == null) {
            return 0;
        }

        try {
            var f = (float) val;
            return toHash(f);
        } catch (ClassCastException e) {
            throw new TableConflictException(1);
        }
    }

    /**
     * 将float转成hash
     *
     * @param f float
     * @return hash
     */
    long toHash(float f) {
        return (long) (f * 1000);
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

            var val = stream.readFloat(JBBPByteOrder.BIG_ENDIAN);
            if (checkVal(val)) {
                return val;
            } else {
                throw new TableConflictException(3);
            }

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

            return checkVal(stream.readFloat(JBBPByteOrder.BIG_ENDIAN));
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public void serialize(OutputStream outputStream, String strVal) throws TableConflictException {

        var stream = new JBBPBitOutputStream(outputStream);

        try {
            if (strVal == null || strVal.isBlank()) {
                stream.writeBytes(new byte[]{1}, 1, JBBPByteOrder.BIG_ENDIAN);
            } else {
                stream.writeBytes(new byte[]{0}, 1, JBBPByteOrder.BIG_ENDIAN);
                var val = Float.parseFloat(strVal);
                if (checkVal(val)) {
                    stream.writeFloat(val, JBBPByteOrder.BIG_ENDIAN);
                } else {
                    throw new TableConflictException(3);
                }
            }

        } catch (IOException e) {
            // fixme 这个给外面可能也不知道如何处理
            throw new RuntimeException();
        } catch (NumberFormatException e) {
            throw new TableConflictException(1);
        }
    }

    @Override
    public void serialize(OutputStream outputStream, Object objVal) throws TableConflictException {
        var stream = new JBBPBitOutputStream(outputStream);

        try {
            if (objVal == null) {
                stream.writeBytes(new byte[]{1}, 1, JBBPByteOrder.BIG_ENDIAN);
            } else {
                stream.writeBytes(new byte[]{0}, 1, JBBPByteOrder.BIG_ENDIAN);
                var val = (float) objVal;
                if (checkVal(val)) {
                    stream.writeFloat(val, JBBPByteOrder.BIG_ENDIAN);
                } else {
                    throw new TableConflictException(3);
                }
            }

        } catch (IOException e) {
            // fixme 这个给外面可能也不知道如何处理
            throw new RuntimeException();
        } catch (NumberFormatException e) {
            throw new TableConflictException(1);
        }
    }

    @Override
    public void insertIndex(String value, long uuid) throws TableConflictException, TableExistenceException {
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
    public List<Long> queryIndex(String value) throws TableExistenceException, TableConflictException {
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
    public Iterator<Pair> queryFirstMeet(String key) throws TableExistenceException, TableConflictException {
        if (index == null) {
            throw new TableExistenceException(6);
        }

        return index.findFirst(strToHash(key));
    }

    @Override
    public Iterator<Pair> queryFirstMeetNotEqual(String key) throws TableExistenceException, TableConflictException {
        if (index == null) {
            throw new TableExistenceException(6);
        }

        return index.findUpperbound(strToHash(key));
    }

    @Override
    public Object strToValue(String str) throws TableConflictException {

        try {
            return Float.parseFloat(str);
        } catch (NumberFormatException e) {
            throw new TableConflictException(1);
        }
    }

    @Override
    public boolean checkObject(Object val) {
        try {
            var res = (float) val;
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    @Override
    public int compare(Object a, Object b) throws TableConflictException {
        try {
            return Float.compare((float) a, (float) b);
        } catch (ClassCastException e) {
            throw new TableConflictException(1);
        }
    }

}
