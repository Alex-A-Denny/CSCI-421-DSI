
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
import page.RecordCodec;
import page.RecordEntry;
import page.RecordEntryType;
import storage.StorageManager;
import table.TableSchema;

public class DMLParser {

    private final StorageManager storageManager;
    private final Catalog catalog;

    public DMLParser(StorageManager storageManager) {
        this.storageManager = storageManager;
        this.catalog = storageManager.catalog;
    }


    // class Attribute{

    // }

    // static class CreateTable{
    //     int id;
    //     String tableName;

    // }

    static class InsertRecord {
        String tableName;
        String[] values;
    }

    static class SelectQuery {
        String tableName;
    }

    public void parseCreateTable(String input) {
    input = input.trim().toLowerCase();
    if (!input.startsWith("create table") || !input.contains("(") || !input.contains(")")) return;

    int openParen = input.indexOf('(');
    int closeParen = input.lastIndexOf(')');
    String tableName = input.substring(12, openParen).trim();
    if (tableName.isEmpty() || catalog.getTable(tableName) != null) return;

    String[] columns = input.substring(openParen + 1, closeParen).split("\\s*,\\s*");
    if (columns.length > TableSchema.MAX_COLUMNS) return; // 确保列数不超过 32

    List<String> names = new ArrayList<>();
    List<RecordEntryType> types = new ArrayList<>();
    List<Integer> sizes = new ArrayList<>();
    List<Object> defaultValues = new ArrayList<>();
    List<Boolean> nullable = new ArrayList<>();
    List<Boolean> unique = new ArrayList<>();

    int primaryKeyIndex = -1;

    for (int i = 0; i < columns.length; i++) {
        String[] parts = columns[i].trim().split("\\s+");
        if (parts.length < 2) return;

        RecordEntryType type;
        int size = -1;
        Object defaultValue = null;

        switch (parts[1]) {
            case "integer" -> type = RecordEntryType.INT;
            case "double" -> type = RecordEntryType.DOUBLE;
            case "boolean" -> type = RecordEntryType.BOOL;
            default -> {
                if (parts[1].startsWith("char(") || parts[1].startsWith("varchar(")) {
                    size = Integer.parseInt(parts[1].replaceAll("\\D", ""));
                    type = parts[1].startsWith("char") ? RecordEntryType.CHAR_FIXED : RecordEntryType.CHAR_VAR;
                } else return;
            }
        }

        boolean isNullable = true, isUnique = false;

        for (int j = 2; j < parts.length; j++) {
            switch (parts[j]) {
                case "notnull" -> isNullable = false;
                case "unique" -> isUnique = true;
                case "primarykey" -> {
                    if (primaryKeyIndex != -1) return;
                    primaryKeyIndex = i;
                    isNullable = false;
                    isUnique = true;
                }
                case "default" -> {
                    if (j + 1 >= parts.length) return;
                    String val = parts[j + 1].replace("\"", "");
                    defaultValue = switch (type) {
                        case INT -> Integer.parseInt(val);
                        case DOUBLE -> Double.parseDouble(val);
                        case BOOL -> Boolean.parseBoolean(val);
                        default -> val;
                    };
                }
            }
        }

        names.add(parts[0]);
        types.add(type);
        sizes.add(size);
        defaultValues.add(defaultValue);
        nullable.add(isNullable);
        unique.add(isUnique);
    }

    if (primaryKeyIndex == -1) return;

    TableSchema schema = new TableSchema(names, types, sizes, defaultValues, unique, nullable, primaryKeyIndex, true);
    catalog.createTable(tableName, new RecordCodec(schema));
}


    // public static void parseCreateTable(String input){

    //    System.out.println(input);
    //    CreateTable ct = new CreateTable();
    //    ct.tableName = input;
    //}

    /**
     * Parses and executes an "INSERT INTO" statement.
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
     * @param input The raw SQL command.
     */
    public void parseSelect(String input) {
        input = input.trim().toLowerCase();
        if (!input.startsWith("select * from")) return;

        String tableName = input.replace("select * from", "").trim();
        Integer tableId = catalog.getTable(tableName);
        if (tableId == null) return;

        List<RecordEntry> records = storageManager.findRecords(tableId, r -> true);

        for (RecordEntry record : records) {
            System.out.println(record.data); 
        }
    }

    
}
