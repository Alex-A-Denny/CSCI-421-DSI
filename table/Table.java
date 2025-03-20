package table;

import catalog.Catalog;
import page.Page;
import page.RecordCodec;
import page.RecordEntry;
import page.RecordEntryType;
import storage.PageBuffer;
import storage.StorageManager;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;

// Author: Spencer Warren

public class Table {

    private final StorageManager storageManager;
    private final Catalog catalog;
    private final PageBuffer pageBuffer;
    private final TableSchema schema;
    private final String name;
    private final int tableId;

    public Table(StorageManager storageManager, int tableId) {
        this.storageManager = storageManager;
        this.catalog = storageManager.catalog;
        this.pageBuffer = storageManager.pageBuffer;
        this.schema = catalog.getCodec(tableId).schema;
        this.name = catalog.getTableName(tableId);
        this.tableId = tableId;
    }

    /**
     * @return the table name
     */
    public String getName() {
        return name;
    }

    /**
     * @return the schema
     */
    public TableSchema getSchema() {
        return schema;
    }

    /**
     * @param predicate the predicate to test
     * @param operation the operation to apply to each matching entry
     */
    public void findMatching(Predicate<RecordEntry> predicate, Consumer<RecordEntry> operation) {
        RecordCodec codec = catalog.getCodec(tableId);
        List<Integer> pages = catalog.getPages(tableId);
        if (pages == null) {
            return;
        }

        for (int pageNum : pages) {
            Page page = getPage(pageNum);
            if (page == null) {
                return;
            }

            page.buf.rewind();
            List<RecordEntry> entries = page.read(codec);
            page.buf.rewind();
            for (RecordEntry entry : entries) {
                if (predicate.test(entry)) {
                    operation.accept(entry);
                }
            }
        }
    }

    /**
     * Deletes all entries matching the predicate
     *
     * @param predicate the predicate
     * @return if successful
     */
    public boolean deleteMatching(Predicate<RecordEntry> predicate) {
        RecordCodec codec = catalog.getCodec(tableId);
        List<Integer> pages = catalog.getPages(tableId);
        if (pages == null) {
            return true;
        }

        for (int pageNum : pages) {
            Page page = getPage(pageNum);
            if (page == null) {
                return false;
            }
            page.buf.rewind();

            List<RecordEntry> entries = page.read(codec);
            entries.removeIf(predicate);

            // wipe the page
            page.buf.rewind();
            page.buf.put(new byte[page.buf.capacity()]);

            // write the new entries to the page
            page.buf.rewind();
            int written = page.write(codec, entries, 0);
            if (written != entries.size()) {
                throw new IllegalStateException("Unable to write a subset of entries to the same page with id " + pageNum);
            }
            page.buf.rewind();
        }
        return true;
    }

    /**
     * @param predicate the predicate to determine what should be updated
     * @param updater   the function applying the update
     * @return if successful
     */
    public boolean updateMatching(Predicate<RecordEntry> predicate, Consumer<RecordEntry> updater) {
        return false;
    }

    /**
     * @param record the entry to add
     * @param checkConstraints if constraints should be checked
     * @return if successful
     */
    public boolean insert(RecordEntry record, boolean checkConstraints) {
        if (checkConstraints && !checkConstraints(record)) {
            return false;
        }

        RecordCodec codec = catalog.getCodec(tableId);
        ByteBuffer encoded = codec.encode(record);
        if (encoded.capacity() >= pageBuffer.pageSize) {
            // pages are too small
            return false;
        }

        List<Integer> pageNums = catalog.getPages(tableId);
        if (pageNums == null) {
            // has no pages, allocate a new one
            Page page = allocateNewPage(-1);
            if (page == null) {
                return false;
            }
            // refresh the page id list
            pageNums = catalog.getPages(tableId);
        }

        for (int i = 0; i < pageNums.size(); i++) {
            int pageNum = pageNums.get(i);
            Page page = getPage(pageNum);
            if (page == null) {
                return false;
            }

            // attempt to determine an insertion point
            page.buf.rewind();
            int count = page.buf.getInt();
            int insertionPoint = -1;
            for (int j = 0; j < count; j++) {
                int prevPos = page.buf.position();
                RecordEntry decoded = codec.decode(page.buf);
                if (insertionPoint == -1) {
                    int cmp = codec.compareRecords(decoded, record);
                    if (cmp == 0) {
                        // same primary key, can't insert
                        return false;
                    }
                    if (cmp > 0) {
                        // shift position
                        insertionPoint = prevPos;
                    }
                }
            }

            if (insertionPoint < 0) {
                // no insertion point found
                if (i + 1 < pageNums.size()) {
                    // there is another page, try that
                    page.buf.rewind();
                    continue;
                } else {
                    // no more pages
                    if (page.buf.position() + encoded.capacity() < page.buf.capacity()) {
                        // enough space for the record at the end, so put it there
                        page.buf.put(encoded);
                        page.buf.rewind();
                        page.buf.putInt(count + 1); // update count
                        page.buf.rewind();
                    } else {
                        // not enough space, need a new one
                        page.buf.rewind();
                        Page newPage = allocateNewPage(-1);  // -1 so it goes at the end since there is no split
                        if (newPage == null) {
                            return false;
                        }
                        newPage.buf.putInt(1); // 1 entry
                        newPage.buf.put(encoded);
                    }
                    return true;
                }
            } else {
                // page insertion point located
                boolean result = shiftInsert(page.buf, encoded, insertionPoint);
                if (!result) {
                    // need to page split
                    // find the halfway split point, need count + 1 because we include the entry to insert
                    int originalPageAmount = (count + 1) / 2;
                    page.buf.position(4); // skip over the count
                    for (int j = 0; j < originalPageAmount; j++) {
                        codec.decode(page.buf); // advance the buf position to the split point
                    }
                    int splitPoint = page.buf.position();

                    // store everything to move
                    List<RecordEntry> list = new ArrayList<>();
                    page.buf.position(4); // skip the count
                    for (int j = 0; j < count; j++) {
                        RecordEntry decoded = codec.decode(page.buf);
                        if (j >= originalPageAmount) {
                            list.add(decoded);
                        }
                    }
                    page.buf.position(splitPoint);
                    // erase everything after the split point
                    page.buf.put(new byte[page.buf.capacity() - splitPoint]);
                    page.buf.rewind();
                    page.buf.putInt(originalPageAmount); // update the count
                    page.buf.rewind();

                    Page newPage = allocateNewPage(page.num + 1);  // new page goes right after old one
                    if (newPage == null) {
                        return false;
                    }
                    newPage.buf.rewind();
                    newPage.buf.putInt(list.size());
                    int written = newPage.write(codec, list, 0);
                    if (written != list.size()) {
                        throw new IllegalStateException("Could not write half the entries to the new page");
                    }

                    if (insertionPoint < splitPoint) {
                        // new entry is in the original page
                        page.buf.rewind();
                        if (shiftInsert(page.buf, encoded, insertionPoint)) {
                            page.buf.rewind();
                            return true;
                        } else {
                            throw new IllegalStateException("Unable to shiftInsert in old page after page split");
                        }
                    } else {
                        // new entry is in the new page
                        // need to compute a new insertion point in the new page
                        newPage.buf.position(4); // skip the count
                        insertionPoint = -1;
                        for (int j = 0; j < list.size(); j++) {
                            int prevPos = newPage.buf.position();
                            RecordEntry decoded = codec.decode(newPage.buf);
                            if (insertionPoint == -1) {
                                int cmp = codec.compareRecords(decoded, record);
                                if (cmp > 0) {
                                    // shift position
                                    insertionPoint = prevPos;
                                }
                            }
                        }
                        if (insertionPoint < 0) {
                            // just add to the end of the page
                            newPage.buf.put(encoded);
                            newPage.buf.rewind();
                            int size = newPage.buf.getInt();
                            newPage.buf.rewind();
                            newPage.buf.putInt(size + 1);
                            newPage.buf.rewind();
                            return true;
                        } else if (shiftInsert(newPage.buf, encoded, insertionPoint)) {
                            newPage.buf.rewind();
                            return true;
                        } else {
                            throw new IllegalStateException("Unable to shiftInsert in new page after page split");
                        }
                    }
                } else {
                    return true;
                }
            }
        }

        return false;
    }

    private boolean shiftInsert(ByteBuffer buf, ByteBuffer entry, int insertionPoint) {
        if (insertionPoint < 0) {
            if (buf.position() + entry.capacity() < buf.capacity()) {
                // enough space for the record at the end, so put it there
                buf.put(entry);
                buf.rewind();
                int size = buf.getInt();
                buf.rewind();
                buf.putInt(size + 1); // increase size by 1
                buf.rewind();
                return true;
            }
        } else {
            if (buf.position() + entry.capacity() < buf.capacity()) {
                // save everything after the insertion point
                int shiftAmount = buf.position() - insertionPoint;
                buf.position(insertionPoint);
                byte[] toShift = new byte[shiftAmount];
                buf.get(toShift);

                // enough room to just shift over
                buf.position(insertionPoint);
                buf.put(entry);
                buf.put(toShift);
                buf.rewind();
                int size = buf.getInt();
                buf.rewind();
                buf.putInt(size + 1); // increase size by 1
                buf.rewind();
                return true;
            }
        }
        return false;
    }

    /**
     * @param name the name of the column
     * @param type the type of the value in the column
     * @param size the size of the value in the column, or -1 for auto sizing
     * @param defaultValue the default value to use in the column
     * @return if successful
     */
    public boolean alterAdd(String name, RecordEntryType type, int size, Object defaultValue) {
        String oldName = catalog.getTableName(tableId);
        if (oldName == null) {
            // table does not exist
            return false;
        }

        RecordCodec oldCodec = catalog.getCodec(tableId);
        if (oldCodec.schema.types.size() + 1 >= TableSchema.MAX_COLUMNS) {
            return false;
        }

        TableSchema schema = oldCodec.schema.copy();
        schema.names.add(name);
        schema.types.add(type);
        schema.sizes.add(size == -1 ? type.size() : size);
        schema.defaultValues.add(defaultValue);
        schema.uniques.add(false);
        schema.nullables.add(true);
        RecordCodec codec = new RecordCodec(schema);

        List<Integer> pages = catalog.getPages(tableId);
        if (pages == null) {
            return true;
        }

        int id = catalog.createTable(oldName + "_alter_add_tmp", codec);
        for (int pageNum : pages) {
            Page page = getPage(pageNum);
            if (page == null) {
                return false;
            }

            page.buf.rewind();
            List<RecordEntry> list = page.read(oldCodec);
            for (RecordEntry entry : list) {
                entry.data.add(defaultValue);
            }

            Page newPage = storageManager.allocateNewPage(id, -1);
            if (newPage == null) {
                return false;
            }

            newPage.buf.rewind();
            int written = newPage.write(codec, list, 0);
            while (written < list.size()) {
                newPage = storageManager.allocateNewPage(id, -1);
                if (newPage == null) {
                    return false;
                }
                written += newPage.write(codec, list, written);
                newPage.buf.rewind();
            }
            newPage.buf.rewind();
        }

        catalog.deleteTable(this.tableId);
        catalog.renameTable(id, oldName);
        for (int pageNum : pages) {
            if (!deletePage(pageNum)) {
                return false;
            }
        }

        return true;
    }

    /**
     * @param index the index of the column
     * @return if successful
     */
    public boolean alterDrop(int index) {
        String oldName = catalog.getTableName(tableId);
        if (oldName == null) {
            // table does not exist
            return false;
        }

        RecordCodec oldCodec = catalog.getCodec(tableId);
        if (index >= oldCodec.schema.types.size()) {
            return false;
        }

        TableSchema schema = oldCodec.schema.copy();
        schema.names.remove(index);
        schema.types.remove(index);
        schema.sizes.remove(index);
        schema.uniques.remove(index);
        schema.nullables.remove(index);
        schema.defaultValues.remove(index);
        RecordCodec codec = new RecordCodec(schema);

        int id = catalog.createTable(oldName + "_alter_add_tmp", codec);
        List<Integer> pages = catalog.getPages(tableId);
        for (int pageNum : pages) {
            Page page = getPage(pageNum);
            if (page == null) {
                return false;
            }

            page.buf.rewind();
            List<RecordEntry> list = page.read(oldCodec);
            for (RecordEntry entry : list) {
                entry.data.remove(index);
            }

            Page newPage = storageManager.allocateNewPage(id, -1);
            if (newPage == null) {
                return false;
            }

            newPage.buf.rewind();
            int written = newPage.write(codec, list, 0);
            while (written < list.size()) {
                newPage = storageManager.allocateNewPage(id, -1);
                if (newPage == null) {
                    return false;
                }
                written += newPage.write(codec, list, written);
                newPage.buf.rewind();
            }
            newPage.buf.rewind();
        }

        catalog.deleteTable(tableId);
        catalog.renameTable(id, oldName);
        for (int pageNum : pages) {
            if (!deletePage(pageNum)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Drop the table
     * @return if successful
     */
    public boolean drop() {
        List<Integer> pages = catalog.getPages(tableId);
        if (pages == null) {
            return false;
        }
        for (int pageNum : pages) {
            try {
                pageBuffer.delete(tableId, pageNum);
            } catch (IOException e) {
                System.err.println("Error deleting page from table " + tableId + ": " + pageNum);
                e.printStackTrace();
            }
        }
        catalog.deleteTable(tableId);
        return true;
    }

    /**
     * Merge two tables
     * @param a the first table
     * @param b the second table
     * @param primaryKeyIndex the index of the primary key within the result of the merged type lists, < 0 for "none"
     * @return the table
     */
    public static Table merge(Table a, Table b, int primaryKeyIndex) {
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
        List<Boolean> uniques = new ArrayList<>();
        for (int i = 0; i < a.schema.uniques.size() + a.schema.uniques.size(); i++) {
            uniques.add(false);
        }
        List<Boolean> nullables = new ArrayList<>();
        for (int i = 0; i < a.schema.nullables.size() + a.schema.nullables.size(); i++) {
            uniques.add(false);
        }
        TableSchema schema = new TableSchema(names, types, sizes, defaultValues, uniques, nullables, primaryKeyIndex, false);

        int id = a.catalog.createTable("merged[" + a.getName() + "," + b.getName() + "]", new RecordCodec(schema));
        Table table = new Table(a.storageManager, id);

        // populate with all combinations of a and b
        RecordCodec aCodec = a.catalog.getCodec(a.tableId);
        List<Integer> aPages = new ArrayList<>(a.catalog.getPages(a.tableId));
        RecordCodec bCodec = b.catalog.getCodec(b.tableId);
        List<Integer> bPages = new ArrayList<>(b.catalog.getPages(b.tableId));

        for (int aPageNum : aPages) {
            Page aPage = a.getPage(aPageNum);
            if (aPage == null) {
                return table;
            }

            aPage.buf.rewind();
            List<RecordEntry> aEntries = aPage.read(aCodec);
            aPage.buf.rewind();
            for (int bPageNum : bPages) {
                Page bPage = b.getPage(bPageNum);
                if (bPage == null) {
                    return table;
                }

                bPage.buf.rewind();
                List<RecordEntry> bEntries = bPage.read(bCodec);
                bPage.buf.rewind();
                for (var aEntry : aEntries) {
                    for (var bEntry : bEntries) {
                        List<Object> entryData = new ArrayList<>(aEntry.data);
                        entryData.addAll(bEntry.data);
                        table.insert(new RecordEntry(entryData), false);
                    }
                }
            }
        }

        return table;
    }

    /**
     * Checks unique constraints for insertion into a table
     *
     * @param record the record to insert
     * @return if the unique constraint is still valid if the record were inserted
     */
    private boolean checkConstraints(RecordEntry record) {
        // TODO this is really inefficient, use the index in phase 3
        List<Integer> pageNums = catalog.getPages(tableId);
        if (pageNums == null) {
            return true;
        }
        RecordCodec codec = catalog.getCodec(tableId);
        for (int pageNum : pageNums) {
            Page page = getPage(pageNum);
            if (page == null) {
                return false;
            }

            page.buf.rewind();
            List<RecordEntry> list = page.read(codec);
            for (int i = 0; i < codec.schema.types.size(); i++) {
                for (RecordEntry entry : list) {
                    Object a = record.data.get(i);
                    if (!schema.nullables.get(i)) {
                        if (a == null) {
                            System.err.println("Error: Null value found in nonnull column '" + schema.names.get(i));
                            return false;
                        }
                    }
                    if (codec.schema.uniques.get(i)) {
                        Object b = entry.data.get(i);
                        if (codec.schema.nullables.get(i) && (a == null || b == null)) {
                            // there can be multiple nulls in a unique column
                            continue;
                        }
                        if (Objects.equals(a, b)) {
                            System.err.println("Error: Duplicate value found in unique column '" + codec.schema.names.get(i) + "': " + a);
                            return false;
                        }
                    }
                }
            }
        }

        return true;
    }

    /**
     * @param pageNum the page id
     * @return the page, or null if an error occurred
     */
    private Page getPage(int pageNum) {
        return storageManager.getPage(tableId, pageNum);
    }

    /**
     * @param sortingIndex the sorting index for the page
     * @return the page, or null if an error occurred
     */
    private Page allocateNewPage(int sortingIndex) {
        return storageManager.allocateNewPage(tableId, sortingIndex);
    }

    /**
     * @param pageNum the page id
     * @return if successful
     */
    private boolean deletePage(int pageNum) {
        return storageManager.deletePage(tableId, pageNum);
    }
}
