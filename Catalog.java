import java.io.*;
import java.util.HashMap;
import java.util.Map;

public class Catalog {
    private Map<String, Integer> tableIDs = new HashMap<>();        // [table name -> table ID]
    private Map<Integer, Schema> tableSchemas = new HashMap<>();    // [table ID -> table schema]
    private Map<Integer, Integer[]> tablePageIDs = new HashMap<>(); // [table ID -> page ids]
    private Map<Integer, Integer> tablePageSize = new HashMap<>();  // [table ID -> page size]
    private String filename; // file location for catalog

    public Catalog(String filename) {
        this.filename = filename;
    }

    public void saveToFile() {
        // Open a file and write the catalog info as binary data
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(filename))) {
            oos.writeObject(tableIDs);
            oos.writeObject(tableSchemas);
            oos.writeObject(tablePageIDs);
            oos.writeObject(tablePageSize);
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
            tableIDs      = (Map<String, Integer>) ois.readObject();
            tableSchemas  = (Map<Integer, Schema>) ois.readObject();
            tablePageIDs  = (Map<Integer, Integer[]>) ois.readObject();
            tablePageSize = (Map<Integer, Integer>) ois.readObject();
        } catch (IOException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    public Integer getTableID(String tableName) {
        return tableIDs.getOrDefault(tableName, null);
    }

    public String getTableName(int tableID) {
        //SEARCH THROUGH THE HASH MAP BCS THERE IS NO BIDIRECTIONAL MAP IN JAVA!
        for (Map.Entry<String, Integer> entry : tableIDs.entrySet()) {
            if (tableID == entry.getValue()) {
                return entry.getKey();
            }
        }
        return null;
    }

    public void addTable(String tableName, int tableID, Schema schema, int pageSize) {
        tableIDs.put(tableName, tableID);
        tableSchemas.put(tableID, schema);
        tablePageIDs.put(tableID, new Integer[0]);
        tablePageSize.put(tableID, pageSize);
    }

    public void removeTable(int tableID) {
        String tableName = getTableName(tableID);
        tableIDs.remove(tableName);
        tableSchemas.remove(tableID);
        tablePageIDs.remove(tableID);
        tablePageSize.remove(tableID);
    }

    public void addPage(int tableID) {
        int numpages = tablePageIDs.get(tableID).length;
        Integer[] newPageList = new Integer[numpages+1];
        //TODO: ADD Logic for ordering new page (THIS DOESNT WORK RIGHT NOW)
        tablePageIDs.put(tableID, newPageList);
    }

    public Schema getSchema(int tableID) {
        return tableSchemas.get(tableID);
    }
}
