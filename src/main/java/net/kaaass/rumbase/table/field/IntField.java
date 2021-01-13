package net.kaaass.rumbase.table.field;

import com.igormaznitsa.jbbp.io.JBBPBitInputStream;
import com.igormaznitsa.jbbp.io.JBBPBitOutputStream;
import com.igormaznitsa.jbbp.io.JBBPByteOrder;
import lombok.NonNull;
import net.kaaass.rumbase.table.exception.TableConflictException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

import static com.igormaznitsa.jbbp.io.JBBPOut.BeginBin;

/**
 * 整型类型的字段
 *
 * @author @KveinAxel
 */
public class IntField extends BaseField {

    public IntField(@NonNull String name) {
        super(name, FieldType.INT);
    }

    @Override
    public boolean checkStr(String valStr) {
        try {
            var val = Integer.parseInt(valStr);
            return checkVal(val);
        } catch (NumberFormatException e) {
            return false;
        }
    }

    boolean checkVal(int val) {
        return true;
    }

    @Override
    public long strToHash(String str) throws TableConflictException {


        try {
            var val = Integer.parseInt(str);

            if (!checkVal(val)) {
                throw new TableConflictException(3);
            }
            return toHash(val);
        } catch (NumberFormatException e) {
            throw new TableConflictException(1);
        }
    }

    long toHash(int val) {
        return val;
    }

    @Override
    public Object deserialize(InputStream inputStream) throws TableConflictException {

        var stream = new JBBPBitInputStream(inputStream);

        try {
            var val = stream.readInt(JBBPByteOrder.BIG_ENDIAN);
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
            return checkVal(stream.readInt(JBBPByteOrder.BIG_ENDIAN));
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public void serialize(OutputStream outputStream, String strVal) throws TableConflictException {

        var stream = new JBBPBitOutputStream(outputStream);

        try {
            var val = Integer.parseInt(strVal);
            if (checkVal(val)) {
                stream.writeInt(val, JBBPByteOrder.BIG_ENDIAN);
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
    public void insertIndex(Object value, long uuid) {

    }
}
