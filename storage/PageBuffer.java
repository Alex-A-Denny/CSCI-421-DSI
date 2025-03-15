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
    public final int pageSize;

    private final int capacity;

    private final Map<Integer, Map<Integer, Page>> map;
    private final ArrayDeque<Page> queue;

    public PageBuffer(Path dbPath, int pageSize, int capacity) {
        this.pagesDir = dbPath.resolve("pages");
        this.pageSize = pageSize;
        this.capacity = capacity;
        this.map = new HashMap<>(capacity);
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

    public Page get(int tableId, int num) throws IOException {
        var tablePages = map.computeIfAbsent(tableId, k -> new HashMap<>());
        Page page = tablePages.get(num);
        if (page == null) {
            page = readOrCreatePage(tableId, num);
            tablePages.put(num, page);
            if (queue.size() == capacity) {
                Page removed = queue.removeFirst();
                var inner = map.get(removed.tableId);
                if (inner != null) {
                    inner.remove(removed.num);
                }
                write(removed);
            }
            queue.addLast(page);
        } else {
            queue.remove(page);
            queue.addLast(page);
        }

        return page;
    }

    public void purge() throws IOException {
        for (var pages : map.values()) {
            for (Page page : pages.values()) {
                write(page);
            }
        }
        queue.clear();
        map.clear();
    }

    private Page readOrCreatePage(int tableId, int pageNum) throws IOException {
        if (!Files.exists(pagesDir)) {
            Files.createDirectories(pagesDir);
        }

        byte[] arr = new byte[pageSize];
        Path tablePath = pagesDir.resolve(String.valueOf(tableId));
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
        return new Page(tableId, pageNum, ByteBuffer.wrap(arr));
    }

    private void write(Page page) throws IOException {
        if (!Files.exists(pagesDir)) {
            Files.createDirectories(pagesDir);
        }

        Path pagePath = pagesDir.resolve(String.valueOf(page.tableId));
        try (RandomAccessFile file = new RandomAccessFile(pagePath.toString(), "rw")) {
            file.seek((long) page.num * pageSize);
            file.write(page.buf.array());
        }
    }

    public void delete(int tableId, int pageNum) throws IOException {
        var pages = map.get(tableId);
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
}
