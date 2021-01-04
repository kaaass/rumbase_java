package net.kaaass.rumbase.table.Field.Visitor;

import com.igormaznitsa.jbbp.io.JBBPOut;
import lombok.AllArgsConstructor;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.kaaass.rumbase.table.Field.FieldWrapper;
import net.kaaass.rumbase.table.Field.FloatField;
import net.kaaass.rumbase.table.Field.IntField;
import net.kaaass.rumbase.table.Field.VarcharField;
import net.kaaass.rumbase.table.exception.NotFoundException;
import net.kaaass.rumbase.table.exception.TypeIncompatibleException;

import java.io.IOException;

@RequiredArgsConstructor
public class AppendToJBBPOutVisitor implements FieldWrapper.Cases<JBBPOut> {

    @NonNull
    private final JBBPOut jbbpOut;

    @Override
    public JBBPOut floatField(FloatField floatField) {
        try {
            return jbbpOut.Bin(floatField.getValue());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return jbbpOut;
    }

    @Override
    public JBBPOut intField(IntField intField) {
        try {
            return jbbpOut.Bin(intField.getValue());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return jbbpOut;
    }

    @Override
    public JBBPOut varcharField(VarcharField varcharField) {
        try {
            return jbbpOut.Bin(varcharField.getValue());
        } catch (IOException e) {
            e.printStackTrace();
        }
        return jbbpOut;
    }
}
