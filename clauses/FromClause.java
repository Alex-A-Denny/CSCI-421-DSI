// Author: Sam Ellis | sae1200@rit.edu

package clauses;

import table.Table;
import storage.StorageManager;
import catalog.Catalog;

import java.util.ArrayList;

public class FromClause {

    /**
     * Merges list of tables together to process the 'FROM ...' part of a SELECT query
     * 
     * @param tableNames Array of strings, each the name of a table to be merged
     * @param SM Predefined Storage Manager
     * @param catalog Predefined Catalog
     * @return single table, merged from given list
     */
    public static Table parseFrom(String[] tableNames, StorageManager SM, Catalog catalog) {

        // Get table IDs
        ArrayList<Integer> tableIds = new ArrayList<>();
        for (String tName : tableNames) {
            Integer Id = catalog.getTable(tName.trim());
            if (Id == null) {
                System.err.println("No such table: " + tName.trim());
                return null;
            }
            tableIds.add(Id);
        }

        // Using IDs, find and store needed tables in an arraylist
        ArrayList<Table> tablesToMerge = new ArrayList<>();
        for (Integer Id : tableIds) {
            Table table = new Table(SM, Id);
            tablesToMerge.add(table);
        }

        // Merge all tables together
        if (tablesToMerge.size() == 1) {
            return tablesToMerge.get(0);
        }

        // This table needs to be deleted at some point after we are done with it
        return Table.mergeN(tablesToMerge);
    }

}
