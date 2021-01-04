package net.kaaass.rumbase.table.Field.Visitor;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.kaaass.rumbase.table.Field.*;
import net.kaaass.rumbase.table.exception.TypeIncompatibleException;

import java.io.IOException;
import static com.igormaznitsa.jbbp.io.JBBPOut.BeginBin;

@RequiredArgsConstructor
public class ValueToRawVisitor implements FieldWrapper.Cases<byte[]> {

    @NonNull
    private final Field value;

    @Override
    public byte[] floatField(FloatField floatField) throws TypeIncompatibleException {
        try {
            return BeginBin().Bin((float) value.getValue()).End().toByteArray();
        } catch (IOException e) {
            // fixme 抛出类型有点奇怪
            throw new TypeIncompatibleException(1);
        }
    }

    @Override
    public byte[] intField(IntField intField) throws TypeIncompatibleException {
        try {
            return BeginBin().Bit((int) value.getValue()).End().toByteArray();
        } catch (IOException e) {
            throw new TypeIncompatibleException(1);
        }
    }

    @Override
    public byte[] varcharField(VarcharField varcharField) throws TypeIncompatibleException {
        char[] v = (char[]) value.getValue();
        try {
            return BeginBin().Bin((short) v.length).Bin(v).End().toByteArray();
        } catch (IOException e) {
            throw new TypeIncompatibleException(1);
        }
    }
}
