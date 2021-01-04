package net.kaaass.rumbase.table.Field.Visitor;

import lombok.AllArgsConstructor;
import net.kaaass.rumbase.table.Field.*;
import net.kaaass.rumbase.table.exception.NotFoundException;
import net.kaaass.rumbase.table.exception.TypeIncompatibleException;

import java.util.List;
import java.util.UUID;

@AllArgsConstructor
public class SearchRangeVisitor implements FieldWrapper.Cases<List<UUID>> {

    private final Field left;

    private final Field right;

    @Override
    public List<UUID> floatField(FloatField floatField) throws TypeIncompatibleException, NotFoundException {
        return null;
    }

    @Override
    public List<UUID> intField(IntField intField) throws TypeIncompatibleException, NotFoundException {
        return null;
    }

    @Override
    public List<UUID> varcharField(VarcharField varcharField) throws TypeIncompatibleException, NotFoundException {
        return null;
    }
}
