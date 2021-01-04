package net.kaaass.rumbase.table.Field.Visitor;

import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.kaaass.rumbase.table.Field.*;
import net.kaaass.rumbase.table.exception.NotFoundException;
import net.kaaass.rumbase.table.exception.TypeIncompatibleException;

@RequiredArgsConstructor
public class CheckValueVisitor implements FieldWrapper.Cases<Boolean> {

    @NonNull
    private final Field value;

    @Override
    public Boolean floatField(FloatField floatField) throws TypeIncompatibleException, NotFoundException {
        // fixme 目前只检测了类型，没有检测字段约束
        return value.getType() == FieldType.FLOAT;
    }

    @Override
    public Boolean intField(IntField intField) throws TypeIncompatibleException, NotFoundException {
        return value.getType() == FieldType.INT;
    }

    @Override
    public Boolean varcharField(VarcharField varcharField) throws TypeIncompatibleException, NotFoundException {
        return value.getType() == FieldType.VARCHAR && ((char[]) value.getValue()).length <= Math.pow(2, 15) - 1;
    }
}
