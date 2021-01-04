package net.kaaass.rumbase.table.Field;

import lombok.NonNull;
import net.kaaass.rumbase.table.Field.Visitor.FieldVisitor;
import net.kaaass.rumbase.table.exception.TypeIncompatibleException;

public class VarcharField extends Field<char[]> {

    private VarcharField(@NonNull String name, char @NonNull [] value) {
        super(name, FieldType.VARCHAR, value);
    }

    @Override
    public void accept(FieldVisitor visitor) {
        visitor.visit(this);
    }

    @Override
    public String getPrepareCode() throws TypeIncompatibleException {
        return "short " + name + "Len;ubyte[" + name + "Len] " + name + ";";
    }

    public static VarcharField withFieldName(@NonNull String name) {
        return new VarcharField(name, new char[0]);
    }

    public static VarcharField withFieldValue(char[] value) {
        return new VarcharField("value", value);
    }

    @Override
    public int compareTo(Field field) {
        return field.getType() != FieldType.VARCHAR ? 0 : new String(value).compareTo(new String((char[]) field.getValue()));
    }
}
