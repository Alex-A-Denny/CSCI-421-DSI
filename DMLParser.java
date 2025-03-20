
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import page.RecordEntry;
import page.RecordEntryType;
import storage.StorageManager;
import table.Table;
import table.TableSchema;

public class DMLParser {

    private final StorageManager storageManager;
    private final Catalog catalog;

    public DMLParser(StorageManager storageManager) {
        this.storageManager = storageManager;
        this.catalog = storageManager.catalog;
    }

    /*public void parseCreateTable(String input) {
    input = input.trim().toLowerCase();
    if (!input.startsWith("create table") || !input.contains("(") || !input.contains(")")) return;

    int openParen = input.indexOf('(');
    int closeParen = input.lastIndexOf(')');
    String tableName = input.substring(12, openParen).trim();
    if (tableName.isEmpty()) {
        System.err.println("Error: table name cannot be empty");
        return;
    }
    if (catalog.getTable(tableName) != null) {
        System.err.println("Error: Table already exists: " + tableName);
        return;
    }

    String[] columns = input.substring(openParen + 1, closeParen).split("\\s*,\\s*");
    if (columns.length > TableSchema.MAX_COLUMNS) return; //  32

    List<String> names = new ArrayList<>();
    List<RecordEntryType> types = new ArrayList<>();
    List<Integer> sizes = new ArrayList<>();
    List<Object> defaultValues = new ArrayList<>();
    List<Boolean> nullable = new ArrayList<>();
    List<Boolean> unique = new ArrayList<>();

    int primaryKeyIndex = -1;

    for (int i = 0; i < columns.length; i++) {
        String[] parts = columns[i].trim().split("\\s+");
        if (parts.length < 2) {
            System.err.println("Error: missing attributes in column definition");
            return;
        }
        if (names.contains(parts[0].trim())) {
            System.err.println("Error: Duplicate column name: " + parts[0].trim());
            return;
        }

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
                } else {
                    System.err.println("Error: invalid type: " + parts[1]);
                    return;
                }
            }
        }

        boolean isNullable = true, isUnique = false;

        for (int j = 2; j < parts.length; j++) {
            switch (parts[j]) {
                case "notnull" -> isNullable = false;
                case "unique" -> isUnique = true;
                case "primarykey" -> {
                    if (primaryKeyIndex != -1) {
                        System.err.println("Error: cannot have more than one primary key");
                        return;
                    }
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

        names.add(parts[0].trim());
        types.add(type);
        sizes.add(size);
        defaultValues.add(defaultValue);
        nullable.add(isNullable);
        unique.add(isUnique);
    }

    if (primaryKeyIndex == -1) {
        System.err.println("Error: must have a primary key");
        return;
    }

    if (names.isEmpty()) {
        System.err.println("Error: must have at least 1 value");
        return;
    }

    TableSchema schema = new TableSchema(names, types, sizes, defaultValues, unique, nullable, primaryKeyIndex, true);
    catalog.createTable(tableName, new RecordCodec(schema));
    System.out.println("Table created.");
}*/

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
            System.err.println("Error: malformed command input: " + input);
            return;
        }

        String tableName = parts[0].replace("insert into", "").trim();
        Integer tableId = catalog.getTable(tableName);
        if (tableId == null) {
            System.err.println("Error: No table exists with name " + tableName);
            return;
        }

        String[] valueEntryParts = parts[1].trim().split(",");
        for (String valuesPart : valueEntryParts) {
            valuesPart = valuesPart.trim();
            if (!valuesPart.startsWith("(") || !valuesPart.endsWith(")")) {
                return;
            }

            valuesPart = valuesPart.substring(1, valuesPart.length() - 1).trim();
            // String[] values = valuesPart.split("\\s* +\\s*");
            List<String> values = new ArrayList<>();
            Matcher matcher = Pattern.compile("\"[^\"]*\"|\\S+").matcher(valuesPart);
            while (matcher.find()) {
                values.add(matcher.group());
            }

            // Convert values into a RecordEntry
            List<Object> recordValues = new ArrayList<>();
            for (String value : values) {
                if (value.matches("-?\\d+")) { 
                    recordValues.add(Integer.parseInt(value));
                } else if (value.matches("-?\\d+\\.\\d+")) { 
                    recordValues.add(Double.parseDouble(value));
                } else if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) { 
                    recordValues.add(Boolean.parseBoolean(value));
                } else if (value.matches("\".*\"")) { 
                    recordValues.add(value.replace("\"", ""));
                } else {
                    System.err.println("Error: Could not parse value: " + value);
                    return;
                }
            }

            TableSchema schema = catalog.getCodec(tableId).schema;
            if (recordValues.size() != schema.types.size()) {
                System.err.println("Error: incorrect number of values, got: " + recordValues.size() + ", need: " + schema.types.size());
                return;
            }
            for (int i = 0; i < schema.types.size(); i++) {
                if (!schema.types.get(i).matchesType(recordValues.get(i))) {
                    System.err.println("Error: value '" + recordValues.get(i) + "' has wrong type: " + recordValues.get(i).getClass().getSimpleName() + ". Need: " + schema.types.get(i).displayStringSimple());
                    return;
                }
            }
            for (int i = 0; i < schema.sizes.size(); i++) {
                if (schema.types.get(i) == RecordEntryType.CHAR_FIXED || schema.types.get(i) == RecordEntryType.CHAR_VAR) {
                    if (recordValues.get(i) instanceof String s && s.length() >= schema.sizes.get(i) / Character.BYTES) {
                        // need to div by char bytes to compare string lengths instead of just using the raw byte amounts
                        System.err.println("Error: Value '" + recordValues.get(i) + "' is too large");
                        return;
                    }
                }
            }

            RecordEntry record = new RecordEntry(recordValues);
            Table table = new Table(storageManager, tableId);
            boolean result = table.insert(record);
            if (!result) {
                return;
            }
        }
        System.out.println("Success.");
    }

    //parses Display Schema and display info
    public void parseDisplay(String input){
        
        if (input.toLowerCase().startsWith("display schema")){
            System.out.println("DB Location: " + storageManager.pageBuffer.getPagesDir());
            System.out.println("Page Size: " + catalog.getPageSize());
            System.out.println("Buffer Size: " + storageManager.pageBuffer.getCapacity());
            System.out.println();

            if (catalog.getTables().isEmpty()) {
                System.out.println("No tables to display");
                return;
            }
            System.out.println("Tables:");
            for ( Integer tableId : catalog.getTables().keySet()) {
                System.out.println("Table name: " + catalog.getTableName(tableId));
                System.out.println("Table schema:\n" + catalog.getCodec(tableId).schema);
                var pages = catalog.getPages(tableId);
                System.out.println("Pages: " + (pages == null ? 0 : pages.size()));
                System.out.println("Records: " + storageManager.findRecordCount(tableId));
                System.out.println("");
            }
        } else if (input.toLowerCase().startsWith("display info")) {
            String tableName = input.substring(input.indexOf("display info") + "display info".length()).trim();
            Integer tableId = catalog.getTable(tableName);
            if (tableId == null) {
                System.err.println("Error: table does not exist: " + tableName);
                return;
            }
            System.out.println("Table name: " + catalog.getTableName(tableId));
            System.out.println("Table schema:\n" + catalog.getCodec(tableId).schema);
            var pages = catalog.getPages(tableId);
            System.out.println("Pages: " + (pages == null ? 0 : pages.size()));
            System.out.println("Records: " + storageManager.findRecordCount(tableId));
        }
        
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
        if (tableId == null) {
            System.err.println("Error: No table exists with name " + tableName);
            return;
        }

        System.out.println(catalog.getCodec(tableId).schema.names);
        Table table = new Table(storageManager, tableId);
        table.findMatching(r -> true, r -> System.out.println(r.data));
    }
}
