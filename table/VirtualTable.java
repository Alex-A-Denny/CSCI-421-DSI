package table;

import page.RecordEntry;
import page.RecordEntryType;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;

// Author: Spencer Warren

public class VirtualTable implements Table {

    private final List<RecordEntry> entries = new ArrayList<>();
    private final TableSchema schema;
    private final String name;

    public VirtualTable(TableSchema schema, String name) {
        this.schema = schema;
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public TableSchema getSchema() {
        return schema;
    }

    @Override
    public void findMatching(Predicate<RecordEntry> predicate, Consumer<RecordEntry> operation) {
        entries.stream()
                .filter(predicate)
                .forEach(operation);
    }

    @Override
    public boolean deleteMatching(Predicate<RecordEntry> predicate) {
        return entries.removeIf(predicate);
    }

    @Override
    public boolean updateMatching(Predicate<RecordEntry> predicate, Consumer<RecordEntry> updater) {
        for (var entry : entries) {
            if (predicate.test(entry)) {
                updater.accept(entry);
                if (!checkConstraints(entry)) {
                    return false;
                }
            }
        }
        entries.stream()
                .filter(predicate)
                .forEach(updater);
        return true;
    }

    @Override
    public boolean insert(RecordEntry entry) {
        for (int i = 0; i < entries.size(); i++) {
            if (compareRecords(entries.get(i), entry) < 0) {
                if (i == entries.size() - 1) {
                    return this.entries.add(entry);
                } else {
                    this.entries.add(i + 1, entry);
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public boolean alterAdd(String name, RecordEntryType type, int size, Object defaultValue) {
        schema.names.add(name);
        schema.types.add(type);
        schema.sizes.add(size == -1 ? type.size() : size);
        schema.defaultValues.add(defaultValue);
        schema.uniques.add(false);
        schema.nullables.add(true);
        for (var entry : entries) {
            entry.data.add(defaultValue);
        }
        return true;
    }

    @Override
    public boolean alterDrop(int index) {
        if (index >= schema.types.size()) {
            return false;
        }

        schema.names.remove(index);
        schema.types.remove(index);
        schema.sizes.remove(index);
        schema.uniques.remove(index);
        schema.nullables.remove(index);
        schema.defaultValues.remove(index);
        for (var entry : entries) {
            entry.data.remove(index);
        }
        return true;
    }

    @Override
    public boolean drop() {
        entries.clear();
        return true;
    }

    /**
     * Merge two tables together
     *
     * @param a the first table
     * @param b the second table
     * @return the merged table
     */
    public static MergedTable merge(VirtualTable a, VirtualTable b) {
        List<String> names = new ArrayList<>();
        a.getSchema().names.stream()
                .map(n -> a.getName() + "." + n)
                .forEach(names::add);
        b.getSchema().names.stream()
                .map(n -> b.getName() + "." + n)
                .forEach(names::add);
        List<RecordEntryType> types = new ArrayList<>(a.schema.types);
        types.addAll(b.schema.types);
        List<Integer> sizes = new ArrayList<>(a.schema.sizes);
        sizes.addAll(b.schema.sizes);
        List<Object> defaultValues = new ArrayList<>(a.schema.defaultValues);
        defaultValues.addAll(b.schema.defaultValues);
        List<Boolean> uniques = new ArrayList<>(a.schema.uniques);
        uniques.addAll(b.schema.uniques);
        List<Boolean> nullables = new ArrayList<>(a.schema.nullables);
        nullables.addAll(b.schema.nullables);
        TableSchema schema = new TableSchema(names, types, sizes, defaultValues, uniques, nullables, 0, false);

        MergedTable table = new MergedTable(schema, "merged[" + a.getName() + "," + b.getName() + "]");
        // all combinations of a and b
        for (RecordEntry aEntry : a.entries) {
            for (RecordEntry bEntry : b.entries) {
                List<Object> resultData = new ArrayList<>(aEntry.data);
                resultData.add(bEntry.data);
                table.insert(new RecordEntry(resultData));
            }
        }
        return table;
    }

    /**
     * @param entry the entry to check
     * @return if constraints are satisfied
     */
    protected boolean checkConstraints(RecordEntry entry) {
        for (int i = 0; i < entry.data.size(); i++) {
            Object a = entry.data.get(i);
            if (!schema.nullables.get(i)) {
                if (a == null) {
                    System.err.println("Error: Null value found in nonnull column '" + schema.names.get(i));
                    return false;
                }
            }
            if (schema.uniques.get(i)) {
                for (var other : entries) {
                    if (entry == other) {
                        continue;
                    }
                    Object b = other.data.get(i);
                    if (a == null && b == null) {
                        continue; // multiple nulls in a unique col are allowed
                    }
                    if (Objects.equals(a, b)) {
                        System.err.println("Error: Duplicate value found in unique column '" + schema.names.get(i) + "': " + a);
                        return false;
                    }
                }
            }
        }
        return true;
    }

    private int compareRecords(RecordEntry e1, RecordEntry e2) {
        Object key1 = e1.data.get(schema.primaryKeyIndex);
        Object key2 = e2.data.get(schema.primaryKeyIndex);
        if (key1 instanceof Integer i) {
            return i.compareTo((Integer) key2);
        } else if (key1 instanceof Double d) {
            return d.compareTo((Double) key2);
        } else if (key1 instanceof Boolean b) {
            return b.compareTo((Boolean) key2);
        } else if (key1 instanceof String s) {
            return s.compareTo((String) key2);
        }
        throw new IllegalStateException("Record primary key had disallowed type");
    }
}
