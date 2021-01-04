package net.kaaass.rumbase.table.Field.Visitor;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import net.kaaass.rumbase.table.Field.*;
import net.kaaass.rumbase.table.exception.NotFoundException;
import net.kaaass.rumbase.table.exception.TypeIncompatibleException;

public class FromFieldVisitor implements FieldVisitor{

    @Getter
    FieldWrapper fieldWrapper;

    public FromFieldVisitor(@NonNull Field field) {
        field.accept(this);
    }

    @Override
    public void visit(FloatField floatField) {
        fieldWrapper = new FieldWrapper() {
            @Override
            public <X> X match(Cases<X> cases) throws TypeIncompatibleException, NotFoundException {
                return cases.floatField(floatField);
            }
        };
    }

    @Override
    public void visit(IntField intField) {
        fieldWrapper = new FieldWrapper() {
            @Override
            public <X> X match(Cases<X> cases) throws TypeIncompatibleException, NotFoundException {
                return cases.intField(intField);
            }
        };
    }


    @Override
    public void visit(VarcharField varcharField) {
        fieldWrapper = new FieldWrapper() {
            @Override
            public <X> X match(Cases<X> cases) throws TypeIncompatibleException, NotFoundException {
                return cases.varcharField(varcharField);
            }
        };
    }
}
