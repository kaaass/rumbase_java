package net.kaaass.rumbase.table.Field;

import com.igormaznitsa.jbbp.JBBPParser;
import com.igormaznitsa.jbbp.model.JBBPFieldArrayUByte;
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
public class VarcharField extends Field<char[]> {

    JBBPParser parser = JBBPParser.prepare("int siz;char[size] " + name + ";");

    private VarcharField(@NonNull String name, char @NonNull [] value) {
        super(name, FieldType.VARCHAR);
    }

    @Override
    public boolean checkStr(String valStr) {
        return true;
    }

    @Override
    public long strToHash(String str) {
        return 0;
    }

    @Override
    public Object deserialize(InputStream inputStream) throws TableConflictException {
        try {
            return parser.parse(inputStream).findFieldForNameAndType(name, JBBPFieldArrayUByte.class);
        } catch (IOException e) {
            throw new TableConflictException(1);
        }
    }

    @Override
    public boolean checkInputStream(InputStream inputStream) {
        try {
            parser.parse(inputStream).findFieldForNameAndType(name, JBBPFieldArrayUByte.class);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public void serialize(OutputStream outputStream, String strVal) {
        try {
            var bytes=  BeginBin().Bin(strVal.length()).Bin(strVal).End().toByteArray();
            outputStream.write(bytes);
        } catch (IOException e) {
            // fixme 这个给外面可能也不知道如何处理
            throw new RuntimeException();
        }
    }

    @Override
    public void insertIndex(Object value, long uuid) {

    }

}
