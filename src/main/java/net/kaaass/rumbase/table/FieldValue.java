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
                var res = 0;
                if (this.type == FieldType.INT) {
                    res = (int) this.getValue() - (int) another.getValue();
                } else if (this.type == FieldType.FLOAT) {
                    var tmp = (float) this.getValue() - (float) another.getValue();
                    if (tmp < 0)
                        res = -1;
                    else if (tmp > 0)
                        res = 1;
                }
                return res;
            }
        } else {
            return 0;
        }
    }
}
