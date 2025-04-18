package page;

// Author: Spencer Warren

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

    public Object parse(String value, int size) {
        return switch (this) {
            case INT -> {
                try {
                    yield Integer.parseInt(value);
                } catch (NumberFormatException e) {
                    System.err.println("Error parsing int from value: " + value);
                    yield null;
                }
            }
            case DOUBLE -> {
                try {
                    yield Double.parseDouble(value);
                } catch (NumberFormatException e) {
                    System.err.println("Error parsing double from value: " + value);
                    yield null;
                }
            }
            case BOOL -> {
                try {
                    yield Boolean.parseBoolean(value);
                } catch (NumberFormatException e) {
                    System.err.println("Error parsing boolean from value: " + value);
                    yield null;
                }
            }
            case CHAR_FIXED -> {
                if (value.getBytes().length <= size) {
                    yield value;
                } else {
                    System.err.println("Error: Fixed length string '" + value + "' of length " + value.length() + " exceeds max size of " + (size / Character.BYTES));
                    yield null;
                }
            }
            case CHAR_VAR -> {
                if (value.getBytes().length <= size) {
                    yield value;
                } else {
                    System.err.println("Error: Variable length string '" + value + "' of length " + value.length() + " exceeds max size of " + (size / Character.BYTES));
                    yield null;
                }
            }
        };
    }

    public String displayString() {
        return switch (this) {
            case INT -> "integer";
            case DOUBLE -> "double";
            case BOOL -> "boolean";
            case CHAR_FIXED -> "char(%d)";
            case CHAR_VAR -> "varchar(%d)";
        };
    }

    public String displayStringSimple() {
        return switch (this) {
            case INT -> "integer";
            case DOUBLE -> "double";
            case BOOL -> "boolean";
            case CHAR_FIXED -> "char";
            case CHAR_VAR -> "varchar";
        };
    }
}
