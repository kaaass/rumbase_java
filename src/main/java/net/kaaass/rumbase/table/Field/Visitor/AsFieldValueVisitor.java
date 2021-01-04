package net.kaaass.rumbase.table.Field.Visitor;

import net.kaaass.rumbase.table.Field.*;
import net.kaaass.rumbase.table.exception.NotFoundException;
import net.kaaass.rumbase.table.exception.TypeIncompatibleException;

public class AsFieldValueVisitor implements FieldWrapper.Cases<Field> {
    @Override
    public Field floatField(FloatField floatField) throws TypeIncompatibleException, NotFoundException {
        return floatField;
    }

    @Override
    public Field intField(IntField intField) throws TypeIncompatibleException, NotFoundException {
        return intField;
    }

    @Override
    public Field varcharField(VarcharField varcharField) throws TypeIncompatibleException, NotFoundException {
        return varcharField;
    }
}
