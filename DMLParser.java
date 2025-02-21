//
// DMLParser.java
// Parses user commands for insert,
// display, and select
//
// Author: Alex A Denny， Beining Zhou
//
////////////////////////////////////////



public class DMLParser {


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

    // Parses an INSERT statement
    public static void parseInsert(String input) {
        System.out.println("Parsing: " + input);

        // Split the statement into table name and values
        String[] parts = input.split("values");
        if (parts.length != 2) {
            System.out.println("Error: Malformed INSERT statement.");
            return;
        }

        // Extract table name
        String tableName = parts[0].replace("insert into", "").trim();
        String valuesPart = parts[1].trim();

        if (!valuesPart.startsWith("(") || !valuesPart.endsWith(")")) {
            System.out.println("Error: Values must be enclosed in parentheses.");
            return;
        }

        // Remove parentheses and split values by commas
        valuesPart = valuesPart.substring(1, valuesPart.length() - 1).trim();
        String[] values = valuesPart.split("\\s*,\\s*"); 

        InsertRecord record = new InsertRecord();
        record.tableName = tableName;
        record.values = values;

        System.out.println("Table: " + record.tableName);
        System.out.println("Values: " + String.join(", ", record.values));

        // StorageManager.insertRecord(record.tableName, record.values);
    }

    // Parses a SELECT statement
    public static void parseSelect(String input) {
        System.out.println("Parsing: " + input);

        // Extract table name
        String tableName = input.replace("select * from", "").trim();
        if (tableName.isEmpty()) {
            System.out.println("Error: Missing table name.");
            return;
        }

        SelectQuery query = new SelectQuery();
        query.tableName = tableName;

        System.out.println("Selecting all records from table: " + query.tableName);

        // StorageManager.selectRecords(query.tableName);
    }
}
