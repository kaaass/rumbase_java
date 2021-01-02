package net.kaaass.rumbase.table;

public enum FieldType {
    INT,
    FLOAT;

    public int getSize() {
        return switch (this) {
            case INT -> 4;
            case FLOAT -> 4;
        };
    }
}
