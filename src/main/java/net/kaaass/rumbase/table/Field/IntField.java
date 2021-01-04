package net.kaaass.rumbase.table.Field;

import lombok.NonNull;
import net.kaaass.rumbase.table.Field.Visitor.FieldVisitor;
import net.kaaass.rumbase.table.exception.TypeIncompatibleException;

public class IntField extends Field<Integer> {
    private IntField(@NonNull String name, @NonNull Integer value) {
        super(name, FieldType.INT, value);
    }

    public static IntField withFieldName(@NonNull String name) {
        return new IntField(name, 0);
    }

    public static IntField withFieldValue(int value) {
        return new IntField("value", value);
    }

    @Override
    public void accept(FieldVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public String getPrepareCode() throws TypeIncompatibleException {
        return "int " + name + ";";
    }

    @Override
    public int compareTo(Field field) {
        return field.getType() == FieldType.INT ? value - (int) field.getValue() : 0;
    }
}
