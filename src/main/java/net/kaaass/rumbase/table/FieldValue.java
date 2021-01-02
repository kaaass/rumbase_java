package net.kaaass.rumbase.table;

public class FieldValue<T> implements Comparable {
    private T value;
    private FieldType type;

    public T getValue() {
        return value;
    }

    public void setValue(T value) {
        this.value = value;
    }

    public FieldType getType() {
        return type;
    }

    public void setType(FieldType type) {
        this.type = type;
    }

    public FieldValue(FieldType type) {
        this.type = type;
    }

    public FieldValue(T value, FieldType type) {
        this.value = value;
        this.type = type;
    }


    @Override
    public int compareTo(Object o) {
        if (o instanceof FieldValue) {
            var another = (FieldValue) o;
            if (this.type != another.type) {
                return 0;
            } else {
                return switch (this.type) {
                    case INT -> (int) this.getValue() - (int) another.getValue();
                    case FLOAT -> {
                        var res = (float) this.getValue() - (float) another.getValue();
                        if (res < 0)
                            yield -1;
                        else if (res == 0)
                            yield 0;
                        else
                            yield 1;
                    }
                };
            }
        } else {
            return 0;
        }
    }
}
