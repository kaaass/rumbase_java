package net.kaaass.rumbase.table.Field.Visitor;

import lombok.AllArgsConstructor;
import net.kaaass.rumbase.table.Field.*;
import net.kaaass.rumbase.table.exception.NotFoundException;
import net.kaaass.rumbase.table.exception.TypeIncompatibleException;

import java.util.Optional;
import java.util.UUID;

@AllArgsConstructor
public class SearchOptionalVisitor implements FieldWrapper.Cases<Optional<UUID>> {
    private final Field value;

    @Override
    public Optional<UUID> floatField(FloatField floatField) throws TypeIncompatibleException, NotFoundException {
        return null;
    }

    @Override
    public Optional<UUID> intField(IntField intField) throws TypeIncompatibleException, NotFoundException {
        return null;
    }

    @Override
    public Optional<UUID> varcharField(VarcharField varcharField) throws TypeIncompatibleException, NotFoundException {
        return null;
    }
}
