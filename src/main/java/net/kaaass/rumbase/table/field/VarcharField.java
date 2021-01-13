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
 * 可变长字符串类型的字段
 *
 * @author @KveinAxel
 */
public class VarcharField extends BaseField {

    /**
     * 字符串长度的限制
     */
    private final int limit;

    public VarcharField(@NonNull String name, int limit) {
        super(name, FieldType.VARCHAR);
        this.limit = limit;
    }

    @Override
    public boolean checkStr(String valStr) {
        return valStr.length() <= this.limit;
    }

    @Override
    public long strToHash(String str) {
        return str.hashCode();
    }

    @Override
    public Object deserialize(InputStream inputStream) throws TableConflictException {

        var stream = new JBBPBitInputStream(inputStream);

        try {
            var str = stream.readString(JBBPByteOrder.BIG_ENDIAN);
            if (checkStr(str)) {
                return str;
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
            var str = stream.readString(JBBPByteOrder.BIG_ENDIAN);
            return checkStr(str);
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
            stream.writeString(strVal, JBBPByteOrder.BIG_ENDIAN);
        } catch (IOException e) {
            // fixme 这个给外面可能也不知道如何处理
            throw new RuntimeException();
        }
    }

    @Override
    public void insertIndex(Object value, long uuid) {

    }

}
