package catalog;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import page.RecordCodec;

public class Catalog {

    private final Map<Integer, String> tables = new HashMap<>();
    private final Map<Integer, RecordCodec> codecs = new HashMap<>();
    private final Map<Integer, List<Integer>> pages = new HashMap<>();
    private int tableCounter = 0;
    private int pageCounter = 0;

    public boolean tableExists(String name) {
        return tables.values().contains(name);
    }

    public int createTable(String name, RecordCodec codec) {
        if (tables.values().contains(name)) {
            throw new IllegalArgumentException("Table already exists: " + name);
        }
        int id = tableCounter++;
        tables.put(id, name);
        codecs.put(id, codec);
        return id;
    }

    public List<Integer> getPages(int tableId) {
        return pages.get(tableId);
    }

    public int requestNewPageId(int tableId, int sortingIndex) {
        int id = pageCounter++;
        var list = pages.computeIfAbsent(tableId, (k) -> new ArrayList<>());
        if (sortingIndex == -1) {
            list.add(id);
        } else {
            list.add(sortingIndex, id);
        }
        // TODO write the new page association to disk
        return id;
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
        tables.remove(tableId);
        codecs.remove(tableId);
        pages.remove(tableId);
    }

    public void changeTableId(int oldId, int newId) {
        tables.put(oldId, tables.get(newId));
        codecs.put(oldId, codecs.get(newId));
        pages.put(oldId, pages.get(newId));
        deleteTable(newId);
    }

    public void renameTable(int id, String name) {
        tables.put(id, name);
    }
}
