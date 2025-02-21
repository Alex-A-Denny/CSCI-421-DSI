package table;

import java.util.ArrayList;
import java.util.List;

import page.RecordEntryType;

public class TableSchema {
    public static final int MAX_COLUMNS = 32; // null bitmask is 4 bytes so we can't support more cols than that

    public final List<RecordEntryType> types;
    public final List<Integer> sizes;
    public final List<Boolean> uniques;
    public final List<Boolean> nullables;
    public final int primaryKeyIndex;

    /**
     * @param types the data types for the codec
     * @param sizes the length of each value, use values < 0 for non string types
     * @param uniques the columns which must be unique
     * @param nullables the columns which can be null
     * @param primaryKeyIndex the index of the primary key
     */
    public TableSchema(List<RecordEntryType> types, List<Integer> sizes, List<Boolean> uniques, List<Boolean> nullables, int primaryKeyIndex) {
        this(types, sizes, uniques, nullables, primaryKeyIndex, true); 
    }

    /**
     * @param types the data types for the codec
     * @param sizes the length of each value, use values < 0 for non string types
     * @param uniques the columns which must be unique
     * @param nullables the columns which can be null
     * @param primaryKeyIndex the index of the primary key
     * @param computeSizes if sizes should be computed
     */
    public TableSchema(List<RecordEntryType> types, List<Integer> sizes, List<Boolean> uniques, List<Boolean> nullables, int primaryKeyIndex, boolean computeSizes) {
        this.types = types;
        this.sizes = sizes;
        this.uniques = uniques;
        this.nullables = nullables;
        this.primaryKeyIndex = primaryKeyIndex;
        if (types.size() != sizes.size()) {
            throw new IllegalArgumentException("length of types does not match length of sizes");
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
        nullables.set(primaryKeyIndex, false);
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
        return new TableSchema(new ArrayList<>(types), new ArrayList<>(sizes), new ArrayList<>(uniques), new ArrayList<>(nullables), primaryKeyIndex, false);
    }
}
