package net.kaaass.rumbase.table.Field.Visitor;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.kaaass.rumbase.table.Field.*;
import net.kaaass.rumbase.table.exception.TypeIncompatibleException;

@RequiredArgsConstructor
public class StrToValueVisitor implements FieldWrapper.Cases<Field> {

    @NonNull
    private final String valStr;

    @Override
    public Field floatField(FloatField floatField) throws TypeIncompatibleException {
        try {
            return FloatField.withFieldValue(Float.parseFloat(valStr));
        } catch (NumberFormatException e) {
            throw new TypeIncompatibleException(1);
        }
    }

    @Override
    public Field intField(IntField intField) throws TypeIncompatibleException {
        try {
            return IntField.withFieldValue(Integer.parseInt(valStr));
        } catch (ArithmeticException e) {
            throw new TypeIncompatibleException(1);
        }
    }

    @Override
    public Field varcharField(VarcharField varcharField) {
        return VarcharField.withFieldValue(valStr.toCharArray());
    }
}
