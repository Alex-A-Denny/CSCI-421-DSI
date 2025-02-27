import java.util.ArrayList;
import java.util.List;

import catalog.Catalog;
import page.RecordCodec;
import page.RecordEntryType;
import storage.StorageManager;
import table.TableSchema;


 //takes in a string//
// DDLParser.java
// Parses user commands for insert,
// display, and select
//
// Author: Alex A Denny
//
////////////////////////////////////////



public class DDLParser {


    /*class Attribute{

    }

    static class CreateTable{
        int id;
        String tableName;

    }


   
    public void parseCreateTable(String input) {
        System.out.println(input);
            CreateTable ct = new CreateTable();
            ct.tableName = input;
    }*/
    


   
    //takes in a string

    private Catalog catalog;
    private StorageManager storageManager;

    public DDLParser(Catalog catalog, StorageManager storageManager) {
        this.catalog = catalog;
        this.storageManager = storageManager;
    }

    public void parseCreateTable(String input) {
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
}


    /*
    Handle "DROP TABLE tableName;"
    */
    public void parseDropTable(String input) {
        // remove semicolon
       input =input.trim();
        if (input.endsWith(";")) {
           input =input.substring(0,input.length() - 1).trim();
        }

        // Tokenize "DROP TABLE <tableName>"
        String[] tokens =input.split("\\s+");
        if (tokens.length < 3) { // check if tokens missing
            System.err.println("Syntax Error: Missing table name in DROP TABLE input");
            return;
        }

        String tableName = tokens[2];

        // Lookup table ID in Catalog
        Integer tableID = catalog.getTable(tableName);
        if (tableID == null) {
            System.err.println("Error: Table '" + tableName + "' does not exist");
            return;
        }

        // Drop the table
        if (storageManager.dropTable(tableID)) {
            System.out.println("Table '" + tableName + "' dropped successfully.");
        } else {
            System.err.println("Error: Unable to drop table: '" + tableName + "'");
        }
    }

    public void parseAlterTable(String lower) {
        int index = lower.indexOf("alter table");
        if (index < 0) {
            System.err.println("Error: ALTER TABLE not at the start");
            return;
        }
        lower = lower.substring(index + "alter table".length()).trim();
        index = lower.indexOf(' ');
        if (index < 0) {
            System.err.println("Error: ALTER TABLE must specify a table name");
            return;
        }
        if (lower.endsWith(";")) {
            lower = lower.substring(0, lower.indexOf(";")).trim();
        }

        String tableName = lower.substring(0, index);
        Integer tableId = catalog.getTable(tableName);
        if (tableId == null) {
            System.err.println("Error: No such table: " + tableName);
            return;
        }

        lower = lower.substring(index + 1).trim();

        index = lower.indexOf(' ');
        if (index < 0) {
            System.err.println("Error: ALTER TABLE must specify DROP or ADD");
            return;
        }
        String op = lower.substring(0, index);
        lower = lower.substring(index + 1).trim();

        TableSchema schema = catalog.getCodec(tableId).schema;
        if ("drop".equals(op)) {
            String columnName = lower;
            int columnIndex = schema.names.indexOf(columnName);
            if (columnIndex < 0) {
                System.err.println("Error: No such column: " + columnName);
                return;
            }
            storageManager.alterDrop(tableId, columnIndex);
        } else if ("add".equals(op)) {
            index = lower.indexOf(' ');
            if (index < 0) {
                System.err.println("Error: ALTER TABLE must specify a column name");
                return;
            }
            String columnName = lower.substring(0, index);
            if (schema.names.contains(columnName)) {
                System.err.println("Error: Duplicate column name: " + columnName);
                return;
            }

            lower = lower.substring(index + 1).trim();

            index = lower.indexOf(' ');
            String typeName;
            if (index < 0) {
                // no default value
                typeName = lower;
            } else {
                // default value
                typeName = lower.substring(0, index);
                lower = lower.substring(index + 1).trim();
            }
            RecordEntryType type;
            int typeSize = -1;
            if ("integer".equals(typeName)) {
                type = RecordEntryType.INT;
            } else if ("double".equals(typeName)) {
                type = RecordEntryType.DOUBLE;
            } else if ("boolean".equals(typeName)) {
                type = RecordEntryType.BOOL;
            } else if (typeName.startsWith("char(")) {
                type = RecordEntryType.CHAR_FIXED;
                int closeIndex = typeName.indexOf(')');
                if (closeIndex < 0) {
                    System.err.println("Error: char(n) must end with a )");
                    return;
                }
                typeName = typeName.substring("char(".length(), closeIndex);
                try {
                    typeSize = Integer.parseInt(typeName);
                } catch (NumberFormatException e) {
                    System.err.println("Error: Unable to parse char(n) size");
                    return;
                }
            } else if (typeName.startsWith("varchar(")) {
                type = RecordEntryType.CHAR_VAR;
                int closeIndex = typeName.indexOf(')');
                if (closeIndex < 0) {
                    System.err.println("Error: char(n) must end with a )");
                    return;
                }
                typeName = typeName.substring("varchar(".length(), closeIndex);
                try {
                    typeSize = Integer.parseInt(typeName);
                } catch (NumberFormatException e) {
                    System.err.println("Error: Unable to parse varchar(n) size");
                    return;
                }
            } else {
                System.err.println("Error: Unknown column type: " + typeName);
                return;
            }
            if (index < 0) {
                storageManager.alterAdd(tableId, columnName, type, typeSize, null);
            } else {
                index = lower.indexOf("default");
                if (index < 0) {
                    System.err.println("Error: ALTER TABLE ADD with these values needs a DEFAULT keyword");
                    return;
                }

                lower = lower.substring(index + "default".length()).trim();
                Object defaultValue;
                if ("null".equals(lower)) {
                    defaultValue = null;
                } else {
                    defaultValue = type.parse(lower, typeSize);
                    if (defaultValue == null) {
                        return;
                    }
                }

                storageManager.alterAdd(tableId, columnName, type, typeSize, defaultValue);
            }
        } else {
            System.err.println("Error: ALTER TABLE operation must be either DROP or ADD");
            return;
        }
    }
}
