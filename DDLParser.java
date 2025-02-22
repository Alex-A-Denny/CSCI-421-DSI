import catalog.Catalog;
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
            System.err.println("Unable to drop table: '" + tableName + "'");
        }
    }

    public void parseAlterTable(String input, String lower) {
        // input is everything after the ALTER TABLE with leading and trailing whitespace trimmed, no semicolon
        int index = lower.indexOf(' ');
        if (index < 0) {
            System.err.println("Error: ALTER TABLE must specify a table name");
            return;
        }

        String tableName = lower.substring(0, index);
        Integer tableId = catalog.getTable(tableName);
        if (tableId == null) {
            System.err.println("Error: No such table: " + tableName);
            return;
        }

        lower = lower.substring(index + 1).trim();
        input = input.substring(index + 1).trim();

        index = lower.indexOf(' ');
        if (index < 0) {
            System.err.println("Error: ALTER TABLE must specify DROP or ADD");
            return;
        }
        String op = lower.substring(0, index);
        lower = lower.substring(index + 1).trim();
        input = input.substring(index + 1).trim();

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
            input = input.substring(index + 1).trim();

            index = lower.indexOf(' ');
            String typeName;
            if (index < 0) {
                // no default value
                typeName = lower;
            } else {
                // default value
                typeName = lower.substring(0, index);
                lower = lower.substring(index + 1).trim();
                input = input.substring(index + 1).trim();
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

                input = input.substring(index + "default".length()).trim();
                lower = lower.substring(index + "default".length()).trim();
                Object defaultValue;
                if ("null".equals(lower)) {
                    defaultValue = null;
                } else {
                    defaultValue = type.parse(input, typeSize);
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
