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

public class PageBuffer {

    private final Path pagesDir;
    public final int pageSize;

    private final int capacity;

    private final Map<Integer, Page> map;
    private final ArrayDeque<Integer> queue;

    public PageBuffer(Path dbPath, int pageSize, int capacity) {
        this.pagesDir = dbPath.resolve("pages");
        this.pageSize = pageSize;
        this.capacity = capacity;
        this.map = new HashMap<>(capacity);
        this.queue = new ArrayDeque<>(capacity);
    }

    public Page get(int pageId) throws IOException {
        Page page = map.get(pageId);
        if (page == null) {
            page = readOrCreatePage(pageId);
            map.put(pageId, page);
            if (queue.size() == capacity) {
                int removedId = queue.removeFirst();
                Page removed = map.remove(removedId);
                write(removed);
            }
            queue.addLast(pageId);
        } else {
            queue.remove(pageId);
            queue.addLast(pageId);
        }

        return page;
    }

    public void purge() throws IOException {
        for (Page value : map.values()) {
            write(value);
        }
        queue.clear();
        map.clear();
    }

    private Page readOrCreatePage(int pageId) throws IOException {
        if (!Files.exists(pagesDir)) {
            Files.createDirectories(pagesDir);
        }

        byte[] arr = new byte[pageSize];
        Path pagePath = pagesDir.resolve(String.valueOf(pageId));
        if (Files.exists(pagePath)) {
            int result = 0;
            try (RandomAccessFile file = new RandomAccessFile(pagePath.toString(), "r")) {
                result = file.read(arr);
            }
            if (result != pageSize) {
                throw new IOException("page was of incorrect size: " + result);
            }
        } else {
            Files.write(pagePath, arr);
        }
        return new Page(pageId, ByteBuffer.wrap(arr));
    }

    public void write(int pageId) throws IOException {
        Page page = map.get(pageId);
        queue.remove(pageId);
        queue.addLast(pageId);
        write(page);
    }

    private void write(Page page) throws IOException {
        if (!Files.exists(pagesDir)) {
            Files.createDirectories(pagesDir);
        }

        Path pagePath = pagesDir.resolve(String.valueOf(page.id));
        try (RandomAccessFile file = new RandomAccessFile(pagePath.toString(), "rw")) {
            file.write(page.buf.array());
        }
    }

    public void delete(int pageId) throws IOException {
        if (map.containsKey(pageId)) {
            map.remove(pageId);
            queue.remove(pageId);
        }

        Path pagePath = pagesDir.resolve(String.valueOf(pageId));
        if (Files.exists(pagePath)) {
            Files.delete(pagePath);
        }
    }
}
