package net.kaaass.rumbase.table;

public enum FieldType {
    INT,
    FLOAT;

    public int getSize() {
        if (this == FieldType.INT) {
            return 4;
        } else if (this == FieldType.FLOAT) {
            return 4;
        } else {
            return 4;
        }
    }
}
