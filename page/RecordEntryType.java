package page;

public enum RecordEntryType {
    INT,
    DOUBLE,
    BOOL,
    CHAR_FIXED,
    CHAR_VAR,
    ;

    public static final RecordEntryType[] VALUES = values();

    public int size() {
        return switch (this) {
            case INT -> Integer.BYTES;
            case DOUBLE -> Double.BYTES;
            case BOOL -> 1;
            case CHAR_FIXED, CHAR_VAR -> Character.BYTES;
        };
    }

    public boolean matchesType(Object o) {
        return switch (this) {
            case INT -> o instanceof Integer;
            case DOUBLE -> o instanceof Double;
            case BOOL -> o instanceof Boolean;
            case CHAR_FIXED, CHAR_VAR -> o instanceof String;
        };
    }
}
