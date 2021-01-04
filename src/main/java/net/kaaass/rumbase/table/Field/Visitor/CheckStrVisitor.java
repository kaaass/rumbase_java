package net.kaaass.rumbase.table.Field.Visitor;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.kaaass.rumbase.table.Field.FieldWrapper;
import net.kaaass.rumbase.table.Field.FloatField;
import net.kaaass.rumbase.table.Field.IntField;
import net.kaaass.rumbase.table.Field.VarcharField;
import net.kaaass.rumbase.table.exception.NotFoundException;
import net.kaaass.rumbase.table.exception.TypeIncompatibleException;

@RequiredArgsConstructor
public class CheckStrVisitor implements FieldWrapper.Cases<Boolean> {

    @NonNull
    private final String valStr;

    @Override
    public Boolean floatField(FloatField floatField) {
        try {
            Float.parseFloat(valStr);
            return true;
        } catch (NumberFormatException e) {
            return false;
        }
    }

    @Override
    public Boolean intField(IntField intField) {
        try {
            Integer.parseInt(valStr);
            return true;
        } catch (ArithmeticException e) {
            return false;
        }
    }

    @Override
    public Boolean varcharField(VarcharField varcharField) {
        return true;
    }
}
