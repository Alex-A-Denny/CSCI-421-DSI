import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class Catalog {
    private Map<String, Integer> tableIDs = new HashMap<>();    // Stores [table name -> table id] relations
    private Map<String, Schema> tableSchemas = new HashMap<>(); // Stores [table name -> table schema] relations
    private String filename; // file location for catalog

    public Catalog(String filename) {
        this.filename = filename;
    }

    public void saveToFile() {
        // Open a file and write the catalog info as binary data
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(filename))) {
            oos.writeObject(tableIDs);
            oos.writeObject(tableSchemas);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @SuppressWarnings("unchecked")
    public void loadFromFile() {
        // Open a file and read/set catalog data
        tableIDs.clear();
        tableSchemas.clear();
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(filename))) {
            tableIDs = (Map<String, Integer>) ois.readObject();
            tableSchemas = (Map<String, Schema>) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public Integer getTableID(String tableName) {
        return tableIDs.getOrDefault(tableName, null);
    }

    public void addTable(String tableName, int tableID, Schema schema) {
        tableIDs.put(tableName, tableID);
        tableSchemas.put(tableName, schema);
    }

    public Schema getSchema(String tableName) {
        return tableSchemas.get(tableName);
    }
}
