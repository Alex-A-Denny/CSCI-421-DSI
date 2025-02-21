
//
// DMLParser.java
// Parses user commands for insert,
// display, and select
//
// Author: Alex A Denny， Beining Zhou
//
////////////////////////////////////////

import catalog.Catalog;
import java.util.ArrayList;
import java.util.List;
import page.RecordEntry;
import storage.StorageManager;

public class DMLParser {

    private final StorageManager storageManager;
    private final Catalog catalog;

    public DMLParser(StorageManager storageManager) {
        this.storageManager = storageManager;
        this.catalog = storageManager.catalog;
    }


    class Attribute{

    }

    static class CreateTable{
        int id;
        String tableName;

    }

    static class InsertRecord {
        String tableName;
        String[] values;
    }

    static class SelectQuery {
        String tableName;
    }


    public static void parseCreateTable(String input){

        System.out.println(input);
        CreateTable ct = new CreateTable();
        ct.tableName = input;
    }

/**
     * Parses and executes an "INSERT INTO" statement.
     * Example:
     * INSERT INTO students VALUES (1, "Alice", true, 3.8);
     * @param input The raw SQL command.
     */
    public void parseInsert(String input) {
        input = input.trim().toLowerCase();
        if (!input.startsWith("insert into") || !input.contains("values")) {
            return;
        }

        // Extract table name and values
        String[] parts = input.split("values");
        if (parts.length != 2) {
            return;
        }

        String tableName = parts[0].replace("insert into", "").trim();
        Integer tableId = catalog.getTable(tableName);
        if (tableId == null) {
            return;
        }

        String valuesPart = parts[1].trim();
        if (!valuesPart.startsWith("(") || !valuesPart.endsWith(")")) {
            return;
        }

        valuesPart = valuesPart.substring(1, valuesPart.length() - 1).trim();
        String[] values = valuesPart.split("\\s*,\\s*");

        // Convert values into a RecordEntry
        List<Object> recordValues = new ArrayList<>();
        for (String value : values) {
            if (value.matches("-?\\d+")) { 
                recordValues.add(Integer.parseInt(value));
            } else if (value.matches("-?\\d+\\.\\d+")) { 
                recordValues.add(Double.parseDouble(value));
            } else if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) { 
                recordValues.add(Boolean.parseBoolean(value));
            } else { 
                recordValues.add(value.replace("\"", ""));
            }
        }

        RecordEntry record = new RecordEntry(recordValues);
        storageManager.insertRecord(tableId, record);
    }

    /**
     * Parses and executes a "SELECT * FROM" statement.
     * Example:
     * SELECT * FROM students;
     * @param input The raw SQL command.
     */
    public void parseSelect(String input) {
        input = input.trim().toLowerCase();
        if (!input.startsWith("select * from")) {
            return;
        }

        String tableName = input.replace("select * from", "").trim();
        Integer tableId = catalog.getTable(tableName);
        if (tableId == null) {
            return;
        }

        storageManager.findRecords(tableId, record -> true);
    }
}
