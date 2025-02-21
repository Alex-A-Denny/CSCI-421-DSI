import catalog.Catalog;
import storage.StorageManager;


 //takes in a string//
// DDLParser.java
// Parses user commands for insert,
// display, and select
//
// Author: Alex A Denny
//
////////////////////////////////////////



public class DDLParser {


    class Attribute{

    }

    static class CreateTable{
        int id;
        String tableName;

    }


   
    public static void parseCreateTable(String input) {
        System.out.println(input);
            CreateTable ct = new CreateTable();
            ct.tableName = input;
    }
    


   
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

}