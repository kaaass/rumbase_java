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


/**
 * 单浮点数类型的字段
 *
 * @author @KveinAxel
 */
public class FloatField extends BaseField {

    public FloatField(@NonNull String name, @NonNull Table parentTable) {
        super(name, FieldType.FLOAT, parentTable);
    }


    @Override
    public void persist(OutputStream stream) {
        var out = new JBBPBitOutputStream(stream);

        try {
            out.writeString(getName(), JBBPByteOrder.BIG_ENDIAN);
            out.writeString(getType().toString(), JBBPByteOrder.BIG_ENDIAN);
            // todo （字段约束）
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean checkStr(String valStr) {
        try {
            return checkVal(Float.parseFloat(valStr));
        } catch (NumberFormatException e) {
            return false;
        }
    }

    boolean checkVal(float val) {
        return true;
    }

    @Override
    public long strToHash(String str) throws TableConflictException {

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

    long toHash(float f) {
        return (long) (f * 1000);
    }

    @Override
    public Object deserialize(InputStream inputStream) throws TableConflictException {

        var stream = new JBBPBitInputStream(inputStream);

        try {
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
            return checkVal(stream.readFloat(JBBPByteOrder.BIG_ENDIAN));
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public void serialize(OutputStream outputStream, String strVal) throws TableConflictException {

        var stream = new JBBPBitOutputStream(outputStream);

        try {
            var val = Float.parseFloat(strVal);
            if (checkVal(val)) {
                stream.writeFloat(val, JBBPByteOrder.BIG_ENDIAN);
            } else {
                throw new TableConflictException(3);
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
    public List<Long> queryIndex(String value) throws TableExistenceException, TableConflictException {
        if (index == null) {
            throw new TableExistenceException(6);
        }

        return index.query(strToHash(value));
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

}
