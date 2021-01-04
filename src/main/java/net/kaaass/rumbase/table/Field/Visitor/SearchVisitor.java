package net.kaaass.rumbase.table.Field.Visitor;

import lombok.AllArgsConstructor;
import net.kaaass.rumbase.table.Field.*;
import net.kaaass.rumbase.table.exception.NotFoundException;
import net.kaaass.rumbase.table.exception.TypeIncompatibleException;

import java.util.UUID;

@AllArgsConstructor
public class SearchVisitor implements FieldWrapper.Cases<UUID> {

    private final Field value;



    @Override
    public UUID floatField(FloatField floatField) throws TypeIncompatibleException, NotFoundException {
        return null;
    }

    @Override
    public UUID intField(IntField intField) throws TypeIncompatibleException, NotFoundException {
        return null;
    }

    @Override
    public UUID varcharField(VarcharField varcharField) throws TypeIncompatibleException, NotFoundException {
        return null;
    }
}
