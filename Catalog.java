import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class Catalog {
    private Map<String, Integer> tableRegistry = new HashMap<>();
    private Map<String, Schema> tableSchemas = new HashMap<>();
    private String filename;

    public Catalog(String filename) {
        this.filename = filename;
    }

    public void saveToFile() {
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(filename))) {
            oos.writeObject(tableRegistry);
            oos.writeObject(tableSchemas);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @SuppressWarnings("unchecked")
    public void loadFromFile() {
        tableRegistry.clear();
        tableSchemas.clear();
        try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(filename))) {
            tableRegistry = (Map<String, Integer>) ois.readObject();
            tableSchemas = (Map<String, Schema>) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public Integer getTableID(String tableName) {
        return tableRegistry.getOrDefault(tableName, null);
    }

    public void addTable(String tableName, int tableID, Schema schema) {
        tableRegistry.put(tableName, tableID);
        tableSchemas.put(tableName, schema);
    }

    public Schema getSchema(String tableName) {
        return tableSchemas.get(tableName);
    }
}
