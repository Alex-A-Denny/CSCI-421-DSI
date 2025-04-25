package table;

import catalog.Catalog;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.function.Predicate;
import page.Page;
import page.RecordCodec;
import page.RecordEntry;
import page.RecordEntryType;
import storage.PageBuffer;
import storage.StorageManager;
import tree.BPPointer;
import tree.BPTree;

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

    public int getTableId() {
        return tableId;
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

        int newTableId = catalog.createTable(name + "_tmp_delete", codec);
        Table table = new Table(storageManager, newTableId);

        for (int pageNum : pages) {
            Page page = getPage(pageNum);
            if (page == null) {
                table.drop();
                return false;
            }
            page.buf.rewind();

            List<RecordEntry> entries = page.read(codec);
            for (RecordEntry entry : entries) {
                if (predicate.test(entry)) {
                    continue;
                }
                boolean success = table.insert(entry, true);
                if (!success) {
                    catalog.deleteTable(newTableId);
                    return false;
                }
            }
            page.buf.rewind();
        }

        catalog.deleteTable(this.tableId);
        catalog.renameTable(newTableId, name);
        for (int pageNum : pages) {
            if (!deletePage(pageNum)) {
                return false;
            }
        }
        return true;
    }

    /**
     * @param predicate the predicate to determine what should be updated
     * @param updater   the function applying the update
     * @return if successful
     */
    public boolean updateMatching(Predicate<RecordEntry> predicate, Consumer<RecordEntry> updater) {
        RecordCodec codec = catalog.getCodec(tableId);
        List<Integer> pages = catalog.getPages(tableId);
        if (pages == null) {
            return true;
        }

        int newTableId = catalog.createTable(name + "_tmp_update", codec);
        Table table = new Table(storageManager, newTableId);

        for (int pageNum : pages) {
            Page page = getPage(pageNum);
            if (page == null) {
                table.drop();
                return false;
            }
            page.buf.rewind();

            List<RecordEntry> entries = page.read(codec);
            for (RecordEntry entry : entries) {
                if (predicate.test(entry)) {
                    updater.accept(entry);
                }
                boolean success = table.insert(entry, true);
                if (!success) {
                    catalog.deleteTable(newTableId);
                    return false;
                }
            }
            page.buf.rewind();
        }

        catalog.deleteTable(this.tableId);
        catalog.renameTable(newTableId, name);
        for (int pageNum : pages) {
            if (!deletePage(pageNum)) {
                return false;
            }
        }

        return true;
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
            // has no pages, allocate a new one and insert immediately
            Page page = allocateNewPage(-1);
            if (page == null) {
                return false;
            }

            page.buf.rewind();
            int written = page.write(codec, Collections.singletonList(record), 0);
            page.buf.rewind();
            if (written != 1) {
                throw new IllegalStateException("Could not write record to empty page");
            }
            if (catalog.indexMode) {
                Object primaryKey = record.data.get(codec.schema.primaryKeyIndex);
                BPTree tree = new BPTree(tableId, codec.schema.types.get(codec.schema.primaryKeyIndex));
                return tree.insert(primaryKey, BPPointer.table(page.num, 0));
            }
            return true;
        }


        if (catalog.indexMode) {
            Object primaryKey = record.data.get(codec.schema.primaryKeyIndex);
            BPTree tree = new BPTree(tableId, codec.schema.types.get(codec.schema.primaryKeyIndex));
            BPPointer pointer = tree.search(primaryKey);
            if (pointer == null) {
                return false;
            }
            // insert
            return insertIndexed(tree, pointer, codec, pageNums, record, encoded);
        }

        return insertIteratePages(codec, pageNums, record, encoded);
    }

    /**
     * Insert into the table using an index
     *
     * @param tree the BPTree for the index
     * @param codec the codec for the table
     * @param pageNums the page numbers for the table
     * @param toInsert the record to insert
     * @param encoded the encoded form of the record to insert
     * @return if insertion was successful
     */
    private boolean insertIndexed(BPTree tree, BPPointer pointer, RecordCodec codec, List<Integer> pageNums, RecordEntry toInsert,
                                  ByteBuffer encoded) {
        int insertionPageNum = pointer.pageNum;
        int insertionPageSortingIndex = pageNums.indexOf(insertionPageNum);
        int insertionIndex = pointer.entryNum;

        if (insertionIndex < 0) {
            throw new IllegalStateException("Leaf node had -1 entry num");
        }

        Page mainPage = getPage(insertionPageNum);
        int currentPageBytes = mainPage.getSize(codec);
        int insertedPageBytes = currentPageBytes + encoded.capacity();
        if (insertedPageBytes < mainPage.buf.capacity()) {
            mainPage.buf.rewind();
            var currentContent = mainPage.read(codec);
            mainPage.buf.rewind();

            RecordEntry start = currentContent.get(insertionIndex);
            Object startPrimaryKey = start.data.get(codec.schema.primaryKeyIndex);
            tree.update(startPrimaryKey,
                    (ptr) -> ptr.pageNum == mainPage.num,
                    (ptr) -> BPPointer.table(mainPage.num, ptr.entryNum + 1));

            // there is room to insert directly
            insertIntoPageDirect(codec, mainPage.buf, encoded, insertionIndex);

            Object primaryKey = toInsert.data.get(codec.schema.primaryKeyIndex);
            return tree.insert(primaryKey, BPPointer.table(mainPage.num, insertionIndex));
        }

        // there is not enough room in the page, requiring a page split

        // determine the split point
        mainPage.buf.rewind();
        var mainPageRecords = mainPage.read(codec);
        mainPage.buf.rewind();

        mainPageRecords.add(insertionIndex, toInsert);

        // split the pages
        int splitIndex = ceilDiv(mainPageRecords.size(), 2);
        List<RecordEntry> leftSplit = new ArrayList<>(splitIndex);
        List<RecordEntry> rightSplit = new ArrayList<>(mainPageRecords.size() - splitIndex);
        for (int i = 0; i < mainPageRecords.size(); i++) {
            RecordEntry record = mainPageRecords.get(i);
            if (i < splitIndex) {
                leftSplit.add(record);
            } else {
                rightSplit.add(record);
            }
        }

        // left page is the main page
        mainPage.buf.put(new byte[pageBuffer.pageSize]); // wipe the current page
        int written = mainPage.write(codec, leftSplit, 0);
        mainPage.buf.rewind();
        if (written != leftSplit.size()) {
            throw new IllegalStateException("Left page did not write the expected amount of entries");
        }

        // right page is the new page
        Page newPage = allocateNewPage(insertionPageSortingIndex + 1); // must go right after the left page
        if (newPage == null) {
            return false;
        }
        written = newPage.write(codec, rightSplit, 0);
        newPage.buf.rewind();
        if (written != rightSplit.size()) {
            throw new IllegalStateException("Right page did not write the expected amount of entries");
        }

        int location = leftSplit.indexOf(toInsert);
        if (location < 0) {
            location = rightSplit.indexOf(toInsert);
        }

        RecordEntry start = leftSplit.get(0);
        Object startPrimaryKey = start.data.get(codec.schema.primaryKeyIndex);
        tree.update(startPrimaryKey,
                (ptr) -> ptr.pageNum == mainPage.num,
                (ptr) -> BPPointer.table(mainPage.num, ptr.entryNum + 1));
        start = rightSplit.get(0);
        startPrimaryKey = start.data.get(codec.schema.primaryKeyIndex);
        tree.update(startPrimaryKey,
                (ptr) -> ptr.pageNum == newPage.num,
                (ptr) -> BPPointer.table(newPage.num, ptr.entryNum + 1));

        Object primaryKey = toInsert.data.get(codec.schema.primaryKeyIndex);
        return tree.insert(primaryKey, BPPointer.table(mainPage.num, location));
    }

    /**
     * Insert into the table by iterating the pages to find the insertion page and index
     * @param codec the codec for the table
     * @param pageNums the page numbers for the table
     * @param toInsert the record to insert
     * @param encoded the encoded form of the record to insert
     * @return if insertion was successful
     */
    private boolean insertIteratePages(RecordCodec codec, List<Integer> pageNums, RecordEntry toInsert,
                                       ByteBuffer encoded) {
        int insertionPageNum = -1;
        int insertionPageSortingIndex = -1;
        int insertionIndex = -1;

        outer:
        for (int pageNumIndex = 0; pageNumIndex < pageNums.size(); pageNumIndex++) {
            int pageNum = pageNums.get(pageNumIndex);
            Page page = getPage(pageNum);
            if (page == null) {
                return false;
            }

            page.buf.rewind();
            var records = page.read(codec);
            page.buf.rewind();

            for (int i = 0; i < records.size(); i++) {
                RecordEntry record = records.get(i);
                int cmp = codec.compareRecords(toInsert, record);
                if (cmp < 0) {
                    // first record where the record to insert is smaller
                    insertionPageNum = pageNum;
                    insertionPageSortingIndex = pageNumIndex;
                    insertionIndex = i;
                    break outer;
                }
            }
        }

        if (insertionIndex < 0) {
            // never found a page to insert into, meaning this record is the biggest
            // make a new page at the end and insert it into there
            Page page = allocateNewPage(-1);
            if (page == null) {
                return false;
            }
            page.buf.putInt(1); // size
            page.buf.put(encoded); // record
            return true;
        }

        Page mainPage = getPage(insertionPageNum);
        int currentPageBytes = mainPage.getSize(codec);
        int insertedPageBytes = currentPageBytes + encoded.capacity();
        if (insertedPageBytes < mainPage.buf.capacity()) {
            // there is room to insert directly
            insertIntoPageDirect(codec, mainPage.buf, encoded, insertionIndex);
            return true;
        }

        // there is not enough room in the page, requiring a page split

        // determine the split point
        mainPage.buf.rewind();
        var mainPageRecords = mainPage.read(codec);
        mainPage.buf.rewind();

        mainPageRecords.add(insertionIndex, toInsert);

        // split the pages
        int splitIndex = ceilDiv(mainPageRecords.size(), 2);
        List<RecordEntry> leftSplit = new ArrayList<>(splitIndex);
        List<RecordEntry> rightSplit = new ArrayList<>(mainPageRecords.size() - splitIndex);
        for (int i = 0; i < mainPageRecords.size(); i++) {
            RecordEntry record = mainPageRecords.get(i);
            if (i < splitIndex) {
                leftSplit.add(record);
            } else {
                rightSplit.add(record);
            }
        }

        // left page is the main page
        mainPage.buf.put(new byte[pageBuffer.pageSize]); // wipe the current page
        int written = mainPage.write(codec, leftSplit, 0);
        mainPage.buf.rewind();
        if (written != leftSplit.size()) {
            throw new IllegalStateException("Left page did not write the expected amount of entries");
        }

        // right page is the new page
        Page newPage = allocateNewPage(insertionPageSortingIndex + 1); // must go right after the left page
        if (newPage == null) {
            return false;
        }
        written = newPage.write(codec, rightSplit, 0);
        newPage.buf.rewind();
        if (written != rightSplit.size()) {
            throw new IllegalStateException("Right page did not write the expected amount of entries");
        }

        return true;
    }

    /**
     * Insert into a page directly
     *
     * @param codec the codec for the page
     * @param buf the buffer for the page
     * @param toInsert the entry to insert
     * @param index the index in which to insert the entry
     */
    private void insertIntoPageDirect(RecordCodec codec, ByteBuffer buf, ByteBuffer toInsert, int index) {
        buf.rewind();
        int recordCount = buf.getInt();

        // advance the buffer up to the insertion point
        for (int i = 0; i < index; i++) {
            codec.decode(buf);
        }
        int insertionBytePosition = buf.position();

        // copy everything after the insertion position to a temporary buffer
        byte[] temp = new byte[buf.capacity() - (insertionBytePosition + toInsert.capacity())];
        buf.get(temp);

        // move back to the new location to insert
        buf.position(insertionBytePosition);
        // insert the new entry, overwriting old data
        buf.put(toInsert);
        // insert the rest after the new entry, overwriting old data
        buf.put(temp);
        buf.rewind();

        // update the record count
        buf.putInt(recordCount + 1);
        buf.rewind();
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

            Page newPage = storageManager.allocateNewTablePage(id, -1);
            if (newPage == null) {
                return false;
            }

            newPage.buf.rewind();
            int written = newPage.write(codec, list, 0);
            while (written < list.size()) {
                newPage = storageManager.allocateNewTablePage(id, -1);
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

            Page newPage = storageManager.allocateNewTablePage(id, -1);
            if (newPage == null) {
                return false;
            }

            newPage.buf.rewind();
            int written = newPage.write(codec, list, 0);
            while (written < list.size()) {
                newPage = storageManager.allocateNewTablePage(id, -1);
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
            catalog.deleteTable(tableId);
            return false;
        }
        for (int pageNum : pages) {
            try {
                pageBuffer.deleteTablePage(tableId, pageNum);
            } catch (IOException e) {
                System.err.println("Error deleting page from table " + tableId + ": " + pageNum);
                e.printStackTrace();
            }
        }
        if (catalog.indexMode) {
            BPTree tree = new BPTree(tableId, schema.types.get(schema.primaryKeyIndex));
            tree.drop();
        }
        catalog.deleteTable(tableId);
        return true;
    }

    /**
     * Merge N tables together
     * @param list the list to merge, will be <strong>mutated</strong>
     * @return the merged table, which must be deleted after use
     */
    public static Table mergeN(ArrayList<Table> list) {
        if (list.isEmpty()) {
            throw new IllegalArgumentException("cannot merge empty list of tables");
        }

        if (list.size() == 1) {
            return list.get(0);
        }
        Table a = list.remove(0);
        Table b = list.remove(0);
        Table merged = merge(a, b, -1);
        if (a.getName().startsWith("Merged[")) {
            a.drop();
        }
        if (b.getName().startsWith("Merged[")) {
            b.drop();
        }
        list.add(0, merged);
        return mergeN(list);
    }

    /**
     * Merge two tables
     * @param a the first table
     * @param b the second table
     * @param primaryKeyIndex the index of the primary key within the result of the merged type lists, < 0 for "none"
     * @return the table
     */
    public static Table merge(Table a, Table b, int primaryKeyIndex) {
        TableSchema schema = TableSchema.merge(a.schema, b.schema, primaryKeyIndex);

        int id = a.catalog.createTable("Merged[" + a.getName() + "," + b.getName() + "]", new RecordCodec(schema));
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
     * Create a new table with only the selected columns
     *
     * @param columnIndices the indices of the columns
     * @return the new table
     */
    public Table toSelected(int[] columnIndices) {
        RecordCodec codec = catalog.getCodec(tableId);
        List<Integer> pageNums = catalog.getPages(tableId);
        if (pageNums == null) {
            return null;
        }
        List<Integer> indices = new ArrayList<>(columnIndices.length);
        for (int i : columnIndices) {
            indices.add(i);
        }
        Table result = new Table(storageManager, catalog.createTable("Selected[" + getName() + "]", new RecordCodec(TableSchema.filter(schema, indices))));
        for (int pageNum : pageNums) {
            Page page = getPage(pageNum);
            if (page == null) {
                result.drop();
                return null;
            }
            List<RecordEntry> list = page.read(codec);
            for (var entry : list) {
                List<Object> filtered = new ArrayList<>(entry.data.size());
                for (int i = 0; i < entry.data.size(); i++) {
                    for (int index : columnIndices) {
                        if (i == index) {
                            filtered.add(entry.data.get(i));
                            break;
                        }
                    }
                }
                result.insert(new RecordEntry(filtered), false);
            }
        }
        return result;
    }
    /**
     * Create a new table with only the filtered rows
     *
     * @param predicate the predicate for which columns should be kept
     * @return the new table
     */
    public Table toFiltered(Predicate<RecordEntry> predicate) {
        RecordCodec codec = catalog.getCodec(tableId);
        List<Integer> pageNums = catalog.getPages(tableId);
        if (pageNums == null) {
            return null;
        }
        Table result = new Table(storageManager, catalog.createTable("Filtered[" + getName() + "]", codec));
        for (int pageNum : pageNums) {
            Page page = getPage(pageNum);
            if (page == null) {
                result.drop();
                return null;
            }
            List<RecordEntry> list = page.read(codec);
            for (var entry : list) {
                boolean passes;
                try {
                    passes = predicate.test(entry);
                } catch (IllegalArgumentException e) {
                    System.err.println("Error: " + e.getMessage());
                    result.drop();
                    return null;
                }
                if (passes) {
                    result.insert(entry, false);
                }
            }
        }
        return result;
    }

    /**
     * Checks unique constraints for insertion into a table
     *
     * @param record the record to insert
     * @return if the unique constraint is still valid if the record were inserted
     */
    private boolean checkConstraints(RecordEntry record) {
        // TODO this is really inefficient
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
        return storageManager.getTablePage(tableId, pageNum);
    }

    /**
     * @param sortingIndex the sorting index for the page
     * @return the page, or null if an error occurred
     */
    private Page allocateNewPage(int sortingIndex) {
        return storageManager.allocateNewTablePage(tableId, sortingIndex);
    }

    /**
     * @param pageNum the page id
     * @return if successful
     */
    private boolean deletePage(int pageNum) {
        return storageManager.deleteTablePage(tableId, pageNum);
    }

    private int ceilDiv(int x, int y){
        return (int) Math.ceil((double)x / (double) y);
    }
}
