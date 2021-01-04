package net.kaaass.rumbase.table.Field.Visitor;

import net.kaaass.rumbase.table.Field.FloatField;
import net.kaaass.rumbase.table.Field.IntField;
import net.kaaass.rumbase.table.Field.VarcharField;

public interface FieldVisitor {
    void visit(IntField intField);

    void visit(FloatField floatField);

    void visit(VarcharField varcharField);
}
