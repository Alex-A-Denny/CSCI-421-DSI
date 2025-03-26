package table;

import java.nio.ByteBuffer;
import java.util.*;

import page.RecordEntryType;

// Author: Spencer Warren

public class TableSchema {
    public static final int MAX_COLUMNS = 32; // null bitmask is 4 bytes so we can't support more cols than that

    public final List<String> names;
    private final Map<String, Integer> columns;
    public final List<RecordEntryType> types;
    public final List<Integer> sizes;
    public final List<Object> defaultValues;
    public final List<Boolean> uniques;
    public final List<Boolean> nullables;
    public final int primaryKeyIndex;

    /**
     * @param names the column names for the table
     * @param types the data types
     * @param sizes the length of each value, use values < 0 for non string types
     * @param defaultValues the default values for each column
     * @param uniques the columns which must be unique
     * @param nullables the columns which can be null
     * @param primaryKeyIndex the index of the primary key
     * @param computeSizes if sizes should be computed
     */
    public TableSchema(List<String> names, List<RecordEntryType> types, List<Integer> sizes, List<Object> defaultValues, List<Boolean> uniques, List<Boolean> nullables, int primaryKeyIndex, boolean computeSizes) {
        this.names = names;
        this.columns = new HashMap<>();
        for (int i = 0; i < names.size(); i++) {
            columns.put(names.get(i), i);
        }
        this.types = types;
        this.sizes = sizes;
        this.defaultValues = defaultValues;
        this.uniques = uniques;
        this.nullables = nullables;
        this.primaryKeyIndex = primaryKeyIndex;
        if (types.size() != sizes.size()) {
            throw new IllegalArgumentException("length of types does not match length of sizes");
        }
        if (types.size() != defaultValues.size()) {
            throw new IllegalArgumentException("length of types does not match length of defaults");
        }
        if (types.size() != uniques.size()) {
            throw new IllegalArgumentException("Types and uniques are not the same length");
        }
        if (types.size() != nullables.size()) {
            throw new IllegalArgumentException("Types and nullables are not the same length");
        }
        if (types.size() > MAX_COLUMNS) {
            throw new IllegalArgumentException("cannot have more than " + MAX_COLUMNS + "types: " + types.size());
        }
        uniques.set(primaryKeyIndex, true);
        if (computeSizes) {
            for (int i = 0; i < sizes.size(); i++) {
                if (sizes.get(i) < 0) {
                    sizes.set(i, types.get(i).size());
                } else {
                    sizes.set(i, sizes.get(i) * types.get(i).size());
                }
            }
        }
    }

    public TableSchema copy() {
        return new TableSchema(new ArrayList<>(names), new ArrayList<>(types), new ArrayList<>(sizes), new ArrayList<>(defaultValues), new ArrayList<>(uniques), new ArrayList<>(nullables), primaryKeyIndex, false);
    }

    public ByteBuffer encode() {
        int size = 1; // 1 byte for the amount of columns
        size += types.size(); // 1 byte per column type
        size += sizes.size(); // 1 byte per size
        size += uniques.size(); // 1 byte per unique value
        size += nullables.size(); // 1 byte per null value
        size += 1; // 1 byte for the primary key index
        for (int i = 0; i < names.size(); i++) {
            size += 4; // length of name
            size += names.get(i).getBytes().length;
        }
        for (int i = 0; i < defaultValues.size(); i++) {
            size += 1; // 1 byte for if the value is null
            if (defaultValues.get(i) == null) {
                continue;
            }
            size += switch (types.get(i)) {
                case INT -> RecordEntryType.INT.size();
                case DOUBLE -> RecordEntryType.DOUBLE.size();
                case BOOL -> RecordEntryType.BOOL.size();
                case CHAR_FIXED -> sizes.get(i);
                case CHAR_VAR -> RecordEntryType.INT.size() + ((String) defaultValues.get(i)).getBytes().length;
            };
        }

        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.put((byte) types.size());
        for (int i = 0; i < types.size(); i++) {
            byte[] arr = names.get(i).getBytes();
            buf.putInt(arr.length);
            buf.put(arr);
            buf.put((byte) types.get(i).ordinal());
            buf.put((byte) sizes.get(i).intValue());
            buf.put((byte) (uniques.get(i) ? 1 : 0));
            buf.put((byte) (nullables.get(i) ? 1 : 0));
            Object defaultValue = defaultValues.get(i);
            if (defaultValue == null) {
                buf.put((byte) 1);
            } else {
                buf.put((byte) 0);
                if (defaultValue instanceof Integer j) {
                    buf.putInt(j);
                } else if (defaultValue instanceof Double d) {
                    buf.putDouble(d);
                } else if (defaultValue instanceof Boolean b) {
                    buf.put((byte) (b ? 1 : 0));
                } else if (defaultValue instanceof String s) {
                    if (types.get(i) == RecordEntryType.CHAR_FIXED) {
                        arr = s.getBytes();
                        // ensure the array is the entire size for fixed length
                        arr = Arrays.copyOf(arr, sizes.get(i));
                        buf.put(arr);
                    } else if (types.get(i) == RecordEntryType.CHAR_VAR) {
                        arr = s.getBytes();
                        buf.putInt(arr.length);
                        buf.put(arr);
                    } else {
                        throw new IllegalStateException("Default value must be CHAR_FIXED or CHAR_VAR for Strings, at index: " + i);
                    }
                }
            }
        }
        buf.put((byte) primaryKeyIndex);
        buf.rewind();

        return buf;
    }

    public static TableSchema decode(ByteBuffer buf) {
        int count = buf.get();
        List<String> names = new ArrayList<>(count);
        List<RecordEntryType> types = new ArrayList<>(count);
        List<Integer> sizes = new ArrayList<>(count);
        List<Boolean> uniques = new ArrayList<>(count);
        List<Boolean> nullables = new ArrayList<>(count);
        List<Object> defaultValues = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            byte[] arr = new byte[buf.getInt()];
            buf.get(arr);
            names.add(new String(arr));
            types.add(RecordEntryType.VALUES[buf.get()]);
            sizes.add((int) buf.get());
            uniques.add(buf.get() == 1 ? true : false);
            nullables.add(buf.get() == 1 ? true : false);
            if (buf.get() == 1) {
                defaultValues.add(null);
            } else {
                Object defaultValue = switch (types.get(i)) {
                    case INT -> buf.getInt();
                    case DOUBLE -> buf.getDouble();
                    case BOOL -> buf.get() == 1;
                    case CHAR_FIXED -> parseCharFixed(buf, sizes.get(i));
                    case CHAR_VAR -> parseCharVar(buf);
                };
                defaultValues.add(defaultValue);
            }
        }
        int primaryKeyIndex = buf.get();
        return new TableSchema(names, types, sizes, defaultValues, uniques, nullables, primaryKeyIndex, false);
    }

    private static String parseCharFixed(ByteBuffer buf, int size) {
        byte[] arr = new byte[size];
        buf.get(arr);

        // determine where padding starts
        int paddingStart = -1;
        for (int j = arr.length - 1; j >= 0; j--) {
            if (arr[j] != 0) {
                paddingStart = j + 1;
                break;
            }
        }
        if (paddingStart < 0) {
            paddingStart = arr.length;
        }

        // remove trailing null bytes
        byte[] trimmed = new byte[paddingStart];
        System.arraycopy(arr, 0, trimmed, 0, paddingStart);
        return new String(trimmed);
    }

    private static String parseCharVar(ByteBuffer buf) {
        int length = buf.getInt();
        byte[] arr = new byte[length];
        buf.get(arr);
        return new String(arr);
    }

    /**
     * @param columnName the name of the column
     * @return the index of the column, otherwise -1
     */
    public int getColumnIndex(String columnName) {
        return columns.getOrDefault(columnName, -1);
    }

    /**
     * Merge two table schemas
     * @param a the first table schema
     * @param b the second table schema
     * @param primaryKeyIndex the index of the primary key within the result of the merged type lists, < 0 for "none"
     * @return the table
     */
    public static TableSchema merge(TableSchema a, TableSchema b, int primaryKeyIndex) {
        List<String> names = new ArrayList<>(a.names);
        names.addAll(b.names);
        List<RecordEntryType> types = new ArrayList<>(a.types);
        types.addAll(b.types);
        List<Integer> sizes = new ArrayList<>(a.sizes);
        sizes.addAll(b.sizes);
        List<Object> defaultValues = new ArrayList<>(a.defaultValues);
        defaultValues.addAll(b.defaultValues);
        List<Boolean> uniques = new ArrayList<>();
        for (int i = 0; i < a.uniques.size() + b.uniques.size(); i++) {
            uniques.add(false);
        }
        List<Boolean> nullables = new ArrayList<>();
        for (int i = 0; i < a.nullables.size() + b.nullables.size(); i++) {
            uniques.add(false);
        }
        return new TableSchema(names, types, sizes, defaultValues, uniques, nullables, primaryKeyIndex, false);
    }

    @Override
    public String toString() {
        List<String> list = new ArrayList<>(types.size());
        for (int i = 0; i < types.size(); i++) {
            String inner = i == primaryKeyIndex ? "primarykey " : "";
            inner += nullables.get(i) ? "" : "notnull ";
            inner += uniques.get(i) ? "unique" : "";
            inner.trim();
            String type = types.get(i).displayString();
            if (types.get(i) == RecordEntryType.CHAR_FIXED || types.get(i) == RecordEntryType.CHAR_VAR) {
                type = String.format(type, sizes.get(i) / Character.BYTES);
            }
            String s = String.format("%s: %s %s", names.get(i), type, inner);
            list.add(s);
        }
        return String.join("\n", list);
    }
}
