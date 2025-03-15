package storage;

import java.io.IOException;
import java.util.List;

import catalog.Catalog;
import page.Page;
import page.RecordCodec;
import page.RecordEntry;

// Author: Spencer Warren

public class StorageManager {
    public final Catalog catalog;
    public final PageBuffer pageBuffer;

    public StorageManager(Catalog catalog, PageBuffer pageBuffer) {
        this.catalog = catalog;
        this.pageBuffer = pageBuffer;
    }

    /**
     * @param tableId the id of the table
     * @return the amount of records in the table
     */
    public int findRecordCount(int tableId) {
        RecordCodec codec = catalog.getCodec(tableId);
        List<Integer> pages = catalog.getPages(tableId);
        if (pages == null) {
            return 0;
        }

        int sum = 0;
        for (int pageNum : pages) {
            Page page = getPage(tableId, pageNum);
            if (page == null) {
                return 0;
            }

            page.buf.rewind();
            List<RecordEntry> entries = page.read(codec);
            sum += entries.size();
        }
        return sum;
    }

    /**
     * @param pageNum the page id
     * @return the page, or null if an error occurred
     */
    public Page getPage(int tableId, int pageNum) {
        try {
            return pageBuffer.get(tableId, pageNum);
        } catch (IOException e) {
            System.err.println("Error reading page with id " + pageNum);
            e.printStackTrace();
            return null;
        }
    }

    /**
     * @param tableId the table the page belongs to
     * @param sortingIndex the sorting index for the page
     * @return the page, or null if an error occurred
     */
    public Page allocateNewPage(int tableId, int sortingIndex) {
        int num = catalog.requestNewPageNum(tableId, sortingIndex);
        try {
            return pageBuffer.get(tableId, num);
        } catch (IOException e) {
            System.err.println("Error retrieving new page with num " + num + " for table " + tableId);
            return null;
        }
    }

    public boolean deletePage(int tableId, int pageNum) {
        try {
            pageBuffer.delete(tableId, pageNum);
            return true;
        } catch (IOException e) {
            System.err.println("Error deleting page from table " + tableId + ": " + pageNum);
            e.printStackTrace();
            return false;
        }
    }
}