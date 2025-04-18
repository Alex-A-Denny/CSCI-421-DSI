
//
// DMLParser.java
// Parses user commands for insert,
// display, and select
//
// Author: Alex A Denny， Beining Zhou
//
////////////////////////////////////////

import catalog.Catalog;
import clauses.FromClause;
import clauses.OrderbyClause;
import clauses.SelectClause;
import clauses.WhereClause;

import java.util.*;
import java.util.function.Predicate;
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
            boolean result = table.insert(record, true);
            if (!result) {
                System.err.println("Insert failed for values: " + recordValues);
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
     * Parses and executes a "SELECT ... FROM ... WHERE ... ORDERBY ..." statement.
     * @param input The raw SQL command.
     */
    public void parseSelect(String input) {
        input = input.trim();
        if (!input.toLowerCase().startsWith("select")) return;
        if (!input.toLowerCase().contains("from")) return;

        String[] orderBySplit = input.split("(?i)orderby", 2);
        String orderByRaw = null;
        if (orderBySplit.length > 1) {
            orderByRaw = orderBySplit[1];
        }
        String[] whereSplit = orderBySplit[0].split("(?i)where", 2);
        String whereRaw = null;
        if (whereSplit.length > 1) {
            whereRaw = whereSplit[1];
        }

        String[] fromSplit = whereSplit[0].split("(?i)from", 2);
        String[] tableNames;
        int[] tableIds;
        if (fromSplit.length > 1) {
            String fromRaw = fromSplit[1];
            tableNames = fromRaw.split(",");
            tableIds = new int[tableNames.length];
            for (int i = 0; i < tableNames.length; i++) {
                tableNames[i] = tableNames[i].strip();
                Integer id = catalog.getTable(tableNames[i]);
                if (id == null) {
                    System.err.println("Error: no such table: " + tableNames[i]);
                    return;
                }
                tableIds[i] = id;
            }
        } else {
            System.err.println("Error: SELECT statement must have FROM clause");
            return;
        }

        String[] selectSplit = fromSplit[0].split("(?i)select", 2);
        String selectRaw;
        if (selectSplit.length > 1) {
            selectRaw = selectSplit[1];
        } else {
            System.err.println("Error: SELECT statement must have SELECT clause");
            return;
        }

        // Create a temporary supertable containing all tables specified
        Table superTable = FromClause.parseFrom(tableNames, storageManager, catalog);
        if (superTable == null) {
            return;
        }

        // filter selected columns
        Table selectedTable = SelectClause.parseSelect(superTable, selectRaw);
        if (selectedTable == null) {
            tryDeleteTempTables(superTable);
            return;
        }

        // Evaluate table based on conditional expression
        Table filteredTable;
        if (whereRaw != null) {
            var eval = WhereClause.parseWhere(whereRaw, Collections.singletonList(selectedTable));
            if (eval == null) {
                tryDeleteTempTables(superTable);
                tryDeleteTempTables(selectedTable);
                return;
            }
            TableSchema schema = selectedTable.getSchema();
            filteredTable = selectedTable.toFiltered(r -> eval.evaluate(r, schema));
            if (filteredTable == null) {
                tryDeleteTempTables(superTable);
                tryDeleteTempTables(selectedTable);
                return;
            }
        } else {
            filteredTable = selectedTable;
        }

        // Sort table based on orderby
        Table orderedTable;
        if (orderByRaw != null) {
            orderedTable = OrderbyClause.parseOrderby(filteredTable, orderByRaw.trim(), storageManager, catalog);
            if (orderedTable == null) {
                tryDeleteTempTables(superTable);
                tryDeleteTempTables(selectedTable);
                tryDeleteTempTables(filteredTable);
                return;
            }
        } else {
            orderedTable = filteredTable;
        }

        // Print selected records
        System.out.println(orderedTable.getSchema().names);
        orderedTable.findMatching(r -> true, System.out::println);

        // Cleanup temporary tables
        tryDeleteTempTables(superTable);
        tryDeleteTempTables(selectedTable);
        tryDeleteTempTables(filteredTable);
        tryDeleteTempTables(orderedTable);
    }

    public void parseDelete(String input) {
        input = input.trim();
        if (!input.startsWith("delete from")) {
            System.err.println("Error: Not a valid DELETE command");
            return;
        }

        String[] parts = input.split("(?i)where", 2);
        String tablePart = parts[0].split("(?i)delete from", 2)[1].trim();
        String whereCondition = (parts.length > 1) ? parts[1].trim() : "";

        Integer tableId = catalog.getTable(tablePart);
        if (tableId == null) {
            System.err.println("Error: Table does not exist: " + tablePart);
            return;
        }

        Table table = new Table(storageManager, tableId);
        TableSchema schema = table.getSchema();

        Predicate<RecordEntry> condition;
        if (!whereCondition.isEmpty()) {
            var eval = WhereClause.parseWhere(whereCondition, Collections.singletonList(table));
            if (eval == null) {
                return;
            }
            try {
                condition = r -> eval.evaluate(r, schema);
            } catch (IllegalArgumentException e) {
                System.err.println("Error: " + e.getMessage());
                return;
            }
        } else {
            condition = r -> true;
        }

        boolean success = table.deleteMatching(condition);
        if (success) {
            System.out.println("Delete operation completed successfully.");
        } else {
            System.err.println("Delete operation failed.");
        }
    }

    private static void tryDeleteTempTables(Table table) {
        if (table.getName().startsWith("Merged[")) {
            table.drop();
        } else if (table.getName().startsWith("Selected[")) {
            table.drop();
        } else if (table.getName().startsWith("Filtered[")) {
            table.drop();
        } else if (table.getName().startsWith("Ordered[")) {
            table.drop();
        }
    }

    public void parseUpdate(String query){
        query = query.trim();
        if(!query.toLowerCase().startsWith("update")){
            System.err.println("Error: Not a valid UPDATE command");
            return;
        }
        if(!query.toLowerCase().contains("set")){
            System.err.println("Error: Not a valid UPDATE command, no set statement");
            return;
        }
        if(!query.toLowerCase().contains("where")){
            System.err.println("Error: Not a valid UPDATE command, no where clause");
            return;
        }

        String[] parts = query.split("(?i)where", 2);
        String tablePart = parts[0].split("\\s+")[1].trim();
        String setString = parts[0].split("(?i)set")[1].trim();
        if (parts.length < 2) {
            System.err.println("Error: cannot find WHERE clause");
            return;
        }
        String whereCondition = parts[1].trim();
        
        Integer tableId = catalog.getTable(tablePart);
        if (tableId == null) {
            System.err.println("Error: Table does not exist: " + tablePart);
            return;
        }
        Table table = new Table(storageManager, tableId);
        var eval = WhereClause.parseWhere(whereCondition, Collections.singletonList(table));
        if (eval == null) {
            return;
        }

        String[] setSplit = setString.split("=", 2);
        String columnName = setSplit[0].strip();
        String value = setSplit[1].strip();

        int index = table.getSchema().getColumnIndex(columnName);
        if (index < 0) {
            System.err.println("Error: no such column: " + columnName);
            return;
        }
        RecordEntryType targetType = table.getSchema().types.get(index);

        Object newValue;
        try {
            if (value.matches("-?\\d+")) {
                newValue = Integer.parseInt(value);
                if (targetType != RecordEntryType.INT) {
                    System.err.println("Error: " + RecordEntryType.INT + " cannot be set to type " + targetType);
                    return;
                }
            } else if (value.matches("-?\\d+\\.\\d+")) {
                newValue = Double.parseDouble(value);
                if (targetType != RecordEntryType.DOUBLE) {
                    System.err.println("Error: " + RecordEntryType.DOUBLE + " cannot be set to type " + targetType);
                    return;
                }
            } else if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) {
                newValue = Boolean.parseBoolean(value);
                if (targetType != RecordEntryType.BOOL) {
                    System.err.println("Error: " + RecordEntryType.BOOL + " cannot be set to type " + targetType);
                    return;
                }
            } else if (value.matches("\".*\"")) {
                newValue = value.replace("\"", "");
                if (targetType != RecordEntryType.CHAR_FIXED && targetType != RecordEntryType.CHAR_VAR) {
                    System.err.println("Error: STRING cannot be set to type " + targetType);
                    return;
                }
                if (((String) newValue).length() > table.getSchema().sizes.get(index) / Character.BYTES) {
                    System.err.println("Error: STRING value is too long " + newValue);
                    return;
                }
            } else {
                System.err.println("Error: Could not parse value: " + value);
                return;
            }
        } catch (Exception e) {
            System.err.println("Error: Could not parse value: " + value);
            return;
        }

        boolean success;
        try {
            success = table.updateMatching(r -> eval.evaluate(r, table.getSchema()),
                r -> r.data.set(index, newValue));
        } catch (IllegalArgumentException e) {
            System.err.println("Error: " + e.getMessage());
            return;
        }

        if (success) {
            System.out.println("Update operation completed successfully.");
        } else {
            System.err.println("Delete operation failed.");
        }
    }
}
