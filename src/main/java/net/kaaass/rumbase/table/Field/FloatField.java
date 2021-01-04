package net.kaaass.rumbase.table.Field;

import lombok.NonNull;
import net.kaaass.rumbase.table.Field.Visitor.FieldVisitor;
import net.kaaass.rumbase.table.exception.TypeIncompatibleException;

public class FloatField extends Field<Float> {
    private FloatField(@NonNull String name, @NonNull Float value) {
        super(name, FieldType.FLOAT, value);
    }

    @Override
    public void accept(FieldVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public String getPrepareCode() throws TypeIncompatibleException {
        return "float " + name + ";";
    }

    public static FloatField withFieldName(@NonNull String name) {
        return new FloatField(name, 0f);
    }

    public static FloatField withFieldValue(float value) {
        return new FloatField("value", value);
    }

    @Override
    public int compareTo(Field field) {
        return field.getType() != FieldType.FLOAT ? 0 :
                value - (float) field.getValue() < 0 ? -1 :
                        value - (float) field.getValue() > 0 ? 1 : 0;
    }
}
