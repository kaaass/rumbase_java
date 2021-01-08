package net.kaaass.rumbase.table.Field;

import com.igormaznitsa.jbbp.JBBPParser;
import com.igormaznitsa.jbbp.model.JBBPFieldInt;
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
public class IntField extends Field<Integer> {

    JBBPParser parser = JBBPParser.prepare("int " + name + ";");

    private IntField(@NonNull String name, @NonNull Integer value) {
        super(name, FieldType.INT);
    }

    @Override
    public boolean checkStr(String valStr) {
        try {
            Integer.parseInt(valStr);
            return true;
        } catch (ArithmeticException e) {
            return false;
        }
    }

    @Override
    public long strToHash(String str) {
        return 0;
    }

    @Override
    public Object deserialize(InputStream inputStream) throws TableConflictException {

        try {
            return parser.parse(inputStream).findFieldForNameAndType(name, JBBPFieldInt.class);
        } catch (IOException e) {
            throw new TableConflictException(1);
        }
    }

    @Override
    public boolean checkInputStream(InputStream inputStream) {
        try {
            parser.parse(inputStream).findFieldForNameAndType(name, JBBPFieldInt.class);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public void serialize(OutputStream outputStream, String strVal) throws TableConflictException {
        try {
            var val = Integer.parseInt(strVal);
            var bytes=  BeginBin().Bin(val).End().toByteArray();
            outputStream.write(bytes);
        } catch (IOException e) {
            // fixme 这个给外面可能也不知道如何处理
            throw new RuntimeException();
        } catch (ArithmeticException e) {
            throw new TableConflictException(1);
        }
    }

    @Override
    public void insertIndex(Object value, long uuid) {

    }
}
