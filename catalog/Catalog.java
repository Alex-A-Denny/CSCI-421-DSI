package catalog;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import page.RecordCodec;
import table.TableSchema;
import tree.BPTree;

// Author: Sam Ellis, Spencer Warren, Alex Denny

public class Catalog {

    public boolean indexMode;

    private final Map<Integer, String> tables = new HashMap<>();
    private final Map<String, Integer> tableNames = new HashMap<>();
    private final Map<Integer, RecordCodec> codecs = new HashMap<>();
    private final Map<Integer, List<Integer>> pages = new HashMap<>();
    private final Map<Integer, Integer> indexByTableId = new HashMap<>();
    private final int pageSize;
    private int tableCounter = 0;
    private int pageCounter = 0;

    public Catalog(int pageSize) {
        this.pageSize = pageSize;
    }

    public Integer getTable(String name) {
        return tableNames.get(name);
    }
    
    public Map<Integer, String> getTables() {
        return tables;
    }

    public int getPageSize() {
        return pageSize;
    }

    public int getTableCounter() {
        return tableCounter;
    }

    public int getPageCounter() {
        return pageCounter;
    }

    public int createTable(String name, RecordCodec codec) {
        if (tableNames.containsKey(name)) {
            throw new IllegalArgumentException("Table already exists: " + name);
        }
        int id = tableCounter++;
        tables.put(id, name);
        tableNames.put(name, id);
        codecs.put(id, codec);
        if (indexMode) {
            indexByTableId.put(id, pageCounter++);
        }
        return id;
    }

    public List<Integer> getPages(int tableId) {
        return pages.get(tableId);
    }

    public int requestNewPageNum(int tableId, int sortingIndex) {
        int num = pageCounter++;
        var list = pages.computeIfAbsent(tableId, (k) -> new ArrayList<>());
        if (sortingIndex == -1) {
            list.add(num);
        } else {
            list.add(sortingIndex, num);
        }
        return num;
    }

    public int requestNewIndexPageNum() {
        return pageCounter++;
    }

    public int getIndexHead(int tableId) {
        return indexByTableId.get(tableId);
    }

    public void setIndexHead(int tableId, int pageNum) {
        indexByTableId.put(tableId, pageNum);
    }

    public RecordCodec getCodec(int tableId) {
        RecordCodec codec = codecs.get(tableId);
        if (codec == null) {
            throw new IllegalStateException("No codec exists for table id " + tableId);
        }
        return codec;
    }

    public String getTableName(int tableId) {
        return tables.get(tableId);
    }

    public void deleteTable(int tableId) {
        tableNames.remove(tables.get(tableId));
        tables.remove(tableId);
        codecs.remove(tableId);
        pages.remove(tableId);
        indexByTableId.remove(tableId);
    }

    public void renameTable(int id, String name) {
        String oldName = tables.get(id);
        tables.put(id, name);
        tableNames.put(name, id);
        tableNames.remove(oldName);
    }

    public ByteBuffer encode() {
        int size = 0;
        size += 4; // page size
        size += 4; // table id counter
        size += 4; // page id counter
        size += 4; // table count
        size += tables.size() * 4; // table ids
        // table names
        for (String s : tableNames.keySet()) {
            size += 4; // string length
            size += s.getBytes().length; // string value
        }
        // codecs
        Map<Integer, ByteBuffer> encodedCodecs = new HashMap<>();
        for (var entry : codecs.entrySet()) {
            ByteBuffer encoded = entry.getValue().schema.encode();
            encoded.rewind();
            encodedCodecs.put(entry.getKey(), encoded);
            size += encoded.capacity();
        }
        // pages
        Map<Integer, ByteBuffer> encodedPages = new HashMap<>();
        for (int id : tables.keySet()) {
            var list = pages.get(id);
            if (list == null) {
                list = new ArrayList<>();
            }
            ByteBuffer encoded = ByteBuffer.allocate(list.size() * 4 + 4);
            encoded.putInt(list.size());
            for (int pageId : list) {
                encoded.putInt(pageId);
            }
            encoded.rewind();
            encodedPages.put(id, encoded);
            size += encoded.capacity();
        }

        // index
        size += 4 * tables.size(); // 1 pageNum (int, 4) per table

        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.putInt(pageSize);
        buf.putInt(tableCounter);
        buf.putInt(pageCounter);
        buf.putInt(tables.size());
        for (int tableId : tables.keySet()) {
            buf.putInt(tableId);
            String tableName = tables.get(tableId);
            buf.putInt(tableName.length());
            buf.put(tableName.getBytes());
            buf.put(encodedCodecs.get(tableId));
            buf.put(encodedPages.get(tableId));
            if (indexByTableId.get(tableId) != null) {
                buf.putInt(indexByTableId.get(tableId));
            } else {
                buf.putInt(-1);
            }
        }

        if (buf.position() != buf.capacity()) {
            throw new IllegalStateException("Unable to fully encode catalog");
        }
        return buf;
    }

    public static Catalog decode(ByteBuffer buf) {
        buf.rewind();
        int pageSize = buf.getInt();
        Catalog catalog = new Catalog(pageSize);
        catalog.tableCounter = buf.getInt();
        catalog.pageCounter = buf.getInt();
        int tableCount = buf.getInt();
        for (int i = 0; i < tableCount; i++) {
            int tableId = buf.getInt();
            byte[] arr = new byte[buf.getInt()];
            buf.get(arr);
            String tableName = new String(arr);
            RecordCodec codec = new RecordCodec(TableSchema.decode(buf));
            int pageCount = buf.getInt();
            List<Integer> pages = new ArrayList<>(pageCount);
            for (int j = 0; j < pageCount; j++) {
                pages.add(buf.getInt());
            }
            catalog.tables.put(tableId, tableName);
            catalog.tableNames.put(tableName, tableId);
            catalog.codecs.put(tableId, codec);
            if (!pages.isEmpty()) {
                catalog.pages.put(tableId, pages);
            }
            int indextableId = buf.getInt();
            if (indextableId != -1) {
                catalog.indexByTableId.put(tableId, indextableId);
            }
        }
        return catalog;
    }
}
