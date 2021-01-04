package net.kaaass.rumbase.table.Field.Visitor;

import com.igormaznitsa.jbbp.model.JBBPFieldArrayUByte;
import com.igormaznitsa.jbbp.model.JBBPFieldFloat;
import com.igormaznitsa.jbbp.model.JBBPFieldInt;
import com.igormaznitsa.jbbp.model.JBBPFieldStruct;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.kaaass.rumbase.table.Field.*;
import net.kaaass.rumbase.table.exception.NotFoundException;
import net.kaaass.rumbase.table.exception.TypeIncompatibleException;

import java.nio.charset.StandardCharsets;

@RequiredArgsConstructor
public class JBBPStructToValueVisitor implements FieldWrapper.Cases<Field> {

    @NonNull
    private final JBBPFieldStruct struct;

    @NonNull
    private final String fieldName;

    @Override
    public Field floatField(FloatField floatField) {
        return FloatField.withFieldValue(struct.findFieldForNameAndType(fieldName, JBBPFieldFloat.class).getAsFloat());
    }

    @Override
    public Field intField(IntField intField) {
        return IntField.withFieldValue(struct.findFieldForNameAndType(fieldName, JBBPFieldInt.class).getAsInt());
    }

    @Override
    public Field varcharField(VarcharField varcharField) {
        var bytes = struct.findFieldForNameAndType(fieldName, JBBPFieldArrayUByte.class).getArray();
        var charArray = new String(bytes, StandardCharsets.US_ASCII).toCharArray();
        return VarcharField.withFieldValue(charArray);
    }
}
