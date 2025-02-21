// Author: Sam Ellis

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import page.RecordEntryType;

public class Catalog {

    private Map<String, Integer> tableIDs = new HashMap<>();     // [table name -> table ID]
    private Map<Integer, Schema> tableSchemas = new HashMap<>(); // [table ID -> schema, page size, pageids]
    private String filename;            // file location for catalog
    private Integer tableIDCounter = 0; // Internal counter used for assigning table IDs

    public Catalog(String filename) {
        this.filename = filename;
    }

    public void saveToFile() {
        int totalBytes = RecordEntryType.INT.size(); // Count number of bytes to allocate (starts with space for 1 int)
        for (Map.Entry<String, Integer> table : tableIDs.entrySet()) {
            byte[] tableName = table.getKey().getBytes();
            byte[] tableSchema = tableSchemas.get(table.getValue()).toBytes();
            totalBytes += tableName.length + tableSchema.length + RecordEntryType.INT.size(); // Name, schema, ID
        }

        ByteBuffer bbuf = ByteBuffer.allocate(totalBytes+RecordEntryType.INT.size()); // Allocate byte buffer
        bbuf.putInt(totalBytes); // Store total bytes at start
        for (Map.Entry<String, Integer> table : tableIDs.entrySet()) {
            bbuf.put(table.getKey().getBytes()); // table name
            bbuf.putInt(table.getValue());       // table ID
            bbuf.put(tableSchemas.get(table.getValue()).toBytes()); // Convert schema object to bytes and insert into buffer
        }

        // TODO: Actually upload the byte buffer into the file using filename
        // Also need to add integer to tell size of schema section for every table
    }

    public void loadFromFile() {
        // TODO
    }

    public Integer getTableID(String tableName) {
        return tableIDs.getOrDefault(tableName, null);
    }

    public String getTableName(int tableID) {
        // Search through the hash map, could also add a reverse map to link ID -> Name. 
        for (Map.Entry<String, Integer> table : tableIDs.entrySet()) {
            if (tableID == table.getValue()) {
                return table.getKey();
            }
        }
        return null;
    }

    public void addTable(String tableName, Schema schema) {
        int tableID = tableIDCounter;
        tableIDs.put(tableName, tableID);
        tableSchemas.put(tableID, schema);
        tableIDCounter += 1;
    }

    public void removeTable(int tableID) {
        String tableName = getTableName(tableID);
        tableIDs.remove(tableName);
        tableSchemas.remove(tableID);
    }

    public void addPage(int tableID) {
        tableSchemas.get(tableID).addPage();
    }

    public Schema getSchema(int tableID) {
        return tableSchemas.get(tableID);
    }
}
