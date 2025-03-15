package table;

import page.RecordEntry;
import page.RecordEntryType;

import java.util.function.Consumer;
import java.util.function.Predicate;

// Author: Spencer Warren

public interface Table {

    /**
     * @return the table name
     */
    String getName();

    /**
     * @return the schema
     */
    TableSchema getSchema();

    /**
     * @param predicate the predicate to test
     * @param operation the operation to apply to each matching entry
     */
    void findMatching(Predicate<RecordEntry> predicate, Consumer<RecordEntry> operation);

    /**
     * Deletes all entries matching the predicate
     *
     * @param predicate the predicate
     * @return if successful
     */
    boolean deleteMatching(Predicate<RecordEntry> predicate);

    /**
     * @param predicate the predicate to determine what should be updated
     * @param updater   the function applying the update
     * @return if successful
     */
    boolean updateMatching(Predicate<RecordEntry> predicate, Consumer<RecordEntry> updater);

    /**
     * @param entry the entry to add
     * @return if successful
     */
    boolean insert(RecordEntry entry);

    /**
     * @param name the name of the column
     * @param type the type of the value in the column
     * @param size the size of the value in the column, or -1 for auto sizing
     * @param defaultValue the default value to use in the column
     * @return if successful
     */
    boolean alterAdd(String name, RecordEntryType type, int size, Object defaultValue);

    /**
     * @param index the index of the column
     * @return if successful
     */
    boolean alterDrop(int index);

    /**
     * Drop the table
     * @return if successful
     */
    boolean drop();
}
