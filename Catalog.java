// Author: Sam Ellis

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Map;
import page.RecordEntryType;

public class Catalog {

    private Map<String, Integer> tableIDs = new HashMap<>();     // [table name -> table ID]
    private Map<Integer, Schema> tableSchemas = new HashMap<>(); // [table ID -> Schema] (see Schema.java)
    private String filePath;            // file location for catalog (ex. Folder/DB1/catalog.bin)
    private Integer tableIDCounter = 0; // Internal counter used for assigning table IDs

    public Catalog(String filePath) {
        this.filePath = filePath;
    }

    public void saveToFile() {
        int totalBytes = RecordEntryType.INT.size(); // Count number of bytes to allocate (starts with space for 1 int)
        for (Map.Entry<String, Integer> table : tableIDs.entrySet()) {
            byte[] tableName = table.getKey().getBytes();
            byte[] tableSchema = tableSchemas.get(table.getValue()).toBytes();
            totalBytes += tableName.length + tableSchema.length + RecordEntryType.INT.size() * 3; //  Name, schema, ID, #of bytes for name, #of bytes for schema
        }

        ByteBuffer bbuf = ByteBuffer.allocate(totalBytes); // Allocate byte buffer
        bbuf.putInt(tableIDCounter); // Store counter at the start
        for (Map.Entry<String, Integer> table : tableIDs.entrySet()) {
            byte[] tableName = table.getKey().getBytes();
            byte[] tableSchema = tableSchemas.get(table.getValue()).toBytes();

            bbuf.putInt(table.getValue());       // table ID
            bbuf.putInt(tableName.length);       // Number of bytes for string name
            bbuf.put(table.getKey().getBytes()); // table name as byte array
            bbuf.putInt(tableSchema.length);     // Number of bytes for schema 
            bbuf.put(tableSchemas.get(table.getValue()).toBytes()); // Schema as byte array
        }

        try (FileOutputStream fos = new FileOutputStream(filePath)) {
            fos.write(bbuf.array()); // Write the entire byte array
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void loadFromFile() {

        this.tableIDs.clear();
        this.tableSchemas.clear();
        ByteBuffer bbuf = fileToBuffer();
        this.tableIDCounter = bbuf.getInt();
        
        while (bbuf.hasRemaining()) {

            int tableID = bbuf.getInt();

            int bytesToRead1 = bbuf.getInt();
            byte[] tableNameBytes = new byte[bytesToRead1];
            bbuf.get(tableNameBytes);
            String tableName = new String(tableNameBytes);

            int bytesToRead2 = bbuf.getInt();
            byte[] tableSchemaBytes = new byte[bytesToRead2];
            bbuf.get(tableSchemaBytes);
            Schema tableSchema = new Schema(tableSchemaBytes);

            this.tableIDs.put(tableName, tableID);
            this.tableSchemas.put(tableID, tableSchema);
        }
    }

    public ByteBuffer fileToBuffer() {
        try (FileInputStream fis = new FileInputStream(filePath)) {
            byte[] binaryData = fis.readAllBytes();
            return ByteBuffer.wrap(binaryData);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
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
