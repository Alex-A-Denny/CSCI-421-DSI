package storage;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Map;

import page.Page;

// Author: Spencer Warren

public class PageBuffer {

    private final Path pagesDir;
    private final Path indexDir;
    public final int pageSize;

    private final int capacity;

    private final Map<Integer, Map<Integer, Page>> tableMap;
    private final Map<Integer, Map<Integer, Page>> indexMap;
    private final ArrayDeque<Page> queue;

    public PageBuffer(Path dbPath, int pageSize, int capacity) {
        this.pagesDir = dbPath.resolve("pages");
        this.indexDir = dbPath.resolve("index");
        this.pageSize = pageSize;
        this.capacity = capacity;
        this.tableMap = new HashMap<>(capacity);
        this.indexMap = new HashMap<>(capacity);
        this.queue = new ArrayDeque<>(capacity);
    }

    public Path getPagesDir() {
        return pagesDir;
    }

    public int getPageSize() {
        return pageSize;
    }

    public int getCapacity() {
        return capacity;
    }

    public Page getTablePage(int tableId, int num) throws IOException {
        var tablePages = tableMap.computeIfAbsent(tableId, k -> new HashMap<>());
        Page page = tablePages.get(num);
        if (page == null) {
            page = readOrCreatePage(tableId, num, false);
            tablePages.put(num, page);
            if (queue.size() == capacity) {
                Page removed = queue.removeFirst();
                var inner = tableMap.get(removed.tableId);
                if (inner != null) {
                    inner.remove(removed.num);
                }
                write(removed, false);
            }
            queue.addLast(page);
        } else {
            queue.remove(page);
            queue.addLast(page);
        }

        return page;
    }

    public Page getIndexPage(int tableId, int num) throws IOException {
        var indexPages = indexMap.computeIfAbsent(tableId, k -> new HashMap<>());
        Page page = indexPages.get(num);
        if (page == null) {
            page = readOrCreatePage(tableId, num, true);
            indexPages.put(num, page);
            if (queue.size() == capacity) {
                Page removed = queue.removeFirst();
                var inner = indexMap.get(removed.tableId);
                if (inner != null) {
                    inner.remove(removed.num);
                }
                write(removed, true);
            }
            queue.addLast(page);
        } else {
            queue.remove(page);
            queue.addLast(page);
        }

        return page;
    }

    public void purge() throws IOException {
        for (var pages : tableMap.values()) {
            for (Page page : pages.values()) {
                write(page, false);
            }
        }
        for (var pages : indexMap.values()) {
            for (Page page : pages.values()) {
                write(page, true);
            }
        }
        queue.clear();
        tableMap.clear();
        indexMap.clear();
    }

    private Page readOrCreatePage(int tableId, int pageNum, boolean isIndex) throws IOException {
        Path dir = getDirForPageType(isIndex);
        if (!Files.exists(dir)) {
            Files.createDirectories(dir);
        }

        byte[] arr = new byte[pageSize];
        Path tablePath = dir.resolve(String.valueOf(tableId));
        if (Files.exists(tablePath)) {
            int result;
            try (RandomAccessFile file = new RandomAccessFile(tablePath.toString(), "rw")) {
                long length = file.length();
                long offset = (long) pageNum * pageSize;
                file.seek(offset);
                if (offset + pageSize > length) {
                    file.write(arr);
                    result = arr.length;
                } else {
                    result = file.read(arr);
                }
            }
            if (result != pageSize) {
                throw new IOException("page was of incorrect size: " + result);
            }
        } else {
            int totalSize = pageNum * pageSize + pageSize;
            Files.write(tablePath, new byte[totalSize]);
        }
        return new Page(tableId, pageNum, isIndex, ByteBuffer.wrap(arr));
    }

    private void write(Page page, boolean isIndex) throws IOException {
        Path dir = getDirForPageType(isIndex);
        if (!Files.exists(dir)) {
            Files.createDirectories(dir);
        }

        Path pagePath = dir.resolve(String.valueOf(page.tableId));
        try (RandomAccessFile file = new RandomAccessFile(pagePath.toString(), "rw")) {
            file.seek((long) page.num * pageSize);
            file.write(page.buf.array());
        }
    }

    public void deleteTablePage(int tableId, int pageNum) throws IOException {
        var pages = tableMap.get(tableId);
        if (pages != null) {
            Page page = pages.remove(pageNum);
            queue.remove(page);
        }

        Path pagePath = pagesDir.resolve(String.valueOf(tableId));
        if (Files.exists(pagePath)) {
            try (RandomAccessFile file = new RandomAccessFile(pagePath.toString(), "rw")) {
                file.seek((long) pageNum * pageSize);
                file.write(new byte[pageSize]);
            }
        }
    }

    public void deleteIndex(int tableId) throws IOException {
        var pages = indexMap.remove(tableId);
        if (pages != null) {
            for (int pageNum : pages.keySet()) {
                Page page = pages.remove(pageNum);
                queue.remove(page);
            }
        }

        Path pagePath = indexDir.resolve(String.valueOf(tableId));
        if (Files.exists(pagePath)) {
            Files.delete(pagePath);
        }
    }

    private Path getDirForPageType(boolean isIndex) {
        if (isIndex) {
            return indexDir;
        }
        return pagesDir;
    }
}
