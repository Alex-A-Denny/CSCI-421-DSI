public class DDLParser {
    //takes in a string

    private static Catalog catalog = new Catalog("Catalog.py");

    /*
    Handle "DROP TABLE tableName;"
    */
    public static void parseDropTable(String input) {
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
        Integer tableID = catalog.getTableID(tableName);
        if (tableID == null) {
            System.err.println("Error: Table '" + tableName + "' does not exist");
            return;
        }

        // Remove table from Catalog
        catalog.removeTable(tableID);

        System.out.println("Table '" + tableName + "' dropped successfully.");
    }
}
