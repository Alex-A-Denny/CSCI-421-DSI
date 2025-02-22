package storage;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.function.Predicate;

import catalog.Catalog;
import page.Page;
import page.RecordCodec;
import page.RecordEntry;
import page.RecordEntryType;
import table.TableSchema;

// Author: Spencer Warren

public class StorageManager {
    public final Catalog catalog;
    public final PageBuffer pageBuffer;

    public StorageManager(Catalog catalog, PageBuffer pageBuffer) {
        this.catalog = catalog;
        this.pageBuffer = pageBuffer;
    }

    /**
     * Runs the DELETE FROM operation
     * 
     * @param tableId the id of the table to delete from
     * @param predicate the predicate for deletion
     * @return if deletion was successful
     */
    public boolean deleteRecords(int tableId, Predicate<RecordEntry> predicate) {
        RecordCodec codec = catalog.getCodec(tableId);
        List<Integer> pages = catalog.getPages(tableId);
        if (pages == null) {
            return true;
        }

        for (int pageId : pages) {
            Page page = getPage(pageId);
            if (page == null) {
                return false;
            }
            page.buf.rewind();

            List<RecordEntry> entries = page.read(codec);
            Iterator<RecordEntry> iterator = entries.iterator();
            while (iterator.hasNext()) {
                RecordEntry entry = iterator.next();
                if (predicate.test(entry)) {
                    iterator.remove();
                }
            }

            // wipe the page
            page.buf.rewind();
            page.buf.put(new byte[page.buf.capacity()]);

            // write the new entries to the page
            page.buf.rewind();
            int written = page.write(codec, entries, 0);
            if (written != entries.size()) {
                throw new IllegalStateException("Unable to write a subset of entries to the same page with id " + pageId);
            }
            page.buf.rewind();
        }
        return true;
    }

    /**
     * Runs the SELECT operation
     * 
     * @param tableId the id of the table
     * @param predicate the predicate for search
     * @return the list of entries matching the predicate, or null if an error occurs
     */
    public List<RecordEntry> findRecords(int tableId, Predicate<RecordEntry> predicate) {
        List<RecordEntry> list = new ArrayList<>();
        RecordCodec codec = catalog.getCodec(tableId);
        List<Integer> pages = catalog.getPages(tableId);
        if (pages == null) {
            return Collections.emptyList();
        }

        for (int pageId : pages) {
            Page page = getPage(pageId);
            if (page == null) {
                return null;
            }

            page.buf.rewind();
            List<RecordEntry> entries = page.read(codec);
            for (RecordEntry entry : entries) {
                if (predicate.test(entry)) {
                    list.add(entry);
                }
            }
        }

        return list;
    }

    /**
     * Runs the INSERT INTO operation
     *
     * @param tableId the id of the table
     * @param record the record to insert
     * @return if the insertion was successful
     */
    public boolean insertRecord(int tableId, RecordEntry record) {
        if (!checkUniqueConstraints(tableId, record)) {
            return false;
        }

        RecordCodec codec = catalog.getCodec(tableId);
        ByteBuffer encoded = codec.encode(record);
        if (encoded.capacity() >= pageBuffer.pageSize) {
            // pages are too small
            return false;
        }

        List<Integer> pageIds = catalog.getPages(tableId);
        if (pageIds == null) {
            // has no pages, allocate a new one
            Page page = allocateNewPage(tableId, -1);
            if (page == null) {
                return false;
            }
            // refresh the page id list
            pageIds = catalog.getPages(tableId);
        }

        for (int i = 0; i < pageIds.size(); i++) {
            int pageId = pageIds.get(i);
            Page page = getPage(pageId);
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
                if (i + 1 < pageIds.size()) {
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
                        Page newPage = allocateNewPage(tableId, -1);  // -1 so it goes at the end since there is no split
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

                    Page newPage = allocateNewPage(tableId, page.id + 1);  // new page goes right after old one
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

    /**
     * Runs the ALTER TABLE ADD operation
     *
     * @param tableId the id of the table
     * @param type the type of the value in the column
     * @param size the size of the value in the column, or -1 for auto sizing
     * @param defaultValue the default value to use in the column
     * @return if successful
     */
    public boolean alterAdd(int tableId, String name, RecordEntryType type, int size, Object defaultValue) {
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
        for (int pageId : pages) {
            Page page = getPage(pageId);
            if (page == null) {
                return false;
            }

            page.buf.rewind();
            List<RecordEntry> list = page.read(oldCodec);
            for (RecordEntry entry : list) {
                entry.data.add(defaultValue);
            }

            Page newPage = allocateNewPage(id, -1);
            if (newPage == null) {
                return false;
            }

            newPage.buf.rewind();
            int written = newPage.write(codec, list, 0);
            while (written < list.size()) {
                newPage = allocateNewPage(id, -1);
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
        for (int pageId : pages) {
            if (!deletePage(pageId)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Runs the ALTER TABLE DROP operation
     *
     * @param tableId the id for the table
     * @param colIndex the index of the column to drop
     * @return if successful
     */
    public boolean alterDrop(int tableId, int colIndex) {
        String oldName = catalog.getTableName(tableId);
        if (oldName == null) {
            // table does not exist
            return false;
        }

        RecordCodec oldCodec = catalog.getCodec(tableId);
        if (colIndex >= oldCodec.schema.types.size()) {
            return false;
        }

        TableSchema schema = oldCodec.schema.copy();
        schema.names.remove(colIndex);
        schema.types.remove(colIndex);
        schema.sizes.remove(colIndex);
        schema.uniques.remove(colIndex);
        schema.nullables.remove(colIndex);
        schema.defaultValues.remove(colIndex);
        RecordCodec codec = new RecordCodec(schema);

        int id = catalog.createTable(oldName + "_alter_add_tmp", codec);
        List<Integer> pages = catalog.getPages(tableId);
        for (int pageId : pages) {
            Page page = getPage(pageId);
            if (page == null) {
                return false;
            }

            page.buf.rewind();
            List<RecordEntry> list = page.read(oldCodec);
            for (RecordEntry entry : list) {
                entry.data.remove(colIndex);
            }

            Page newPage = allocateNewPage(id, -1);
            if (newPage == null) {
                return false;
            }

            newPage.buf.rewind();
            int written = newPage.write(codec, list, 0);
            while (written < list.size()) {
                newPage = allocateNewPage(id, -1);
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
        for (int pageId : pages) {
            if (!deletePage(pageId)) {
                return false;
            }
        }

        return true;
    }

    /**
     * Runs the DROP TABLE operation
     *
     * @param tableId the table to drop
     * @return if successful
     */
    public boolean dropTable(int tableId) {
        List<Integer> pages = catalog.getPages(tableId);
        if (pages == null) {
            return false;
        }
        for (int pageId : pages) {
            try {
                pageBuffer.delete(pageId);
            } catch (IOException e) {
                System.err.println("Error deleting page " + pageId);
                e.printStackTrace();
            }
        }
        catalog.deleteTable(tableId);
        return true;
    }

    /**
     * Insert an entry into a buffer, shifting later entries if needed
     * 
     * @param buf the buffer to insert into, positioned at the end of the written data
     * @param entry the entry to insert
     * @param insertionPoint the position in the buffer to insert into
     * @return if insertion was successful
     */
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
     * Checks unique contraints for insertion into a table
     * 
     * @param tableId the id of the table to insert into
     * @param record the record to insert
     * @return if the unique constraint is still valid if the record were inserted
     */
    private boolean checkUniqueConstraints(int tableId, RecordEntry record) {
        // TODO this is really inefficient, use the index in phase 2
        List<Integer> pageIds = catalog.getPages(tableId);
        if (pageIds == null) {
            return true;
        }
        RecordCodec codec = catalog.getCodec(tableId);
        for (int pageId : pageIds) {
            Page page = getPage(pageId);
            if (page == null) {
                return false;
            }

            page.buf.rewind();
            List<RecordEntry> list = page.read(codec);
            for (int i = 0; i < codec.schema.types.size(); i++) {
                for (RecordEntry entry : list) {
                    if (codec.schema.uniques.get(i)) {
                        Object a = record.data.get(i);
                        Object b = entry.data.get(i);
                        if (codec.schema.nullables.get(i) && (a == null || b == null)) {
                            // there can be multiple nulls in a unique column
                            continue;
                        }
                        if (a.equals(b)) {
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
     * @param pageId the page id
     * @return the page, or null if an error occurred
     */
    private Page getPage(int pageId) {
        try {
            return pageBuffer.get(pageId);
        } catch (IOException e) {
            System.err.println("Error reading page with id " + pageId);
            e.printStackTrace();
            return null;
        }
    }

    /**
     * @param tableId the id of the table the page belongs to
     * @param sortingIndex the sorting index for the page
     * @return the page, or null if an error occurred
     */
    private Page allocateNewPage(int tableId, int sortingIndex) {
        int newPageId = catalog.requestNewPageId(tableId, sortingIndex);
        try {
            return pageBuffer.get(newPageId);
        } catch (IOException e) {
            System.err.println("Error retrieving new page with id " + newPageId + " for table " + tableId);
            return null;
        }
    }

    private boolean deletePage(int id) {
        try {
            pageBuffer.delete(id);
            return true;
        } catch (IOException e) {
            System.err.println("Error deleting page: " + id);
            e.printStackTrace();
            return false;
        }
    }
}