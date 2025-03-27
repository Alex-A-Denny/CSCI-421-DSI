package clauses;

import table.Table;
import storage.StorageManager;
import catalog.Catalog;

import java.util.ArrayList;

public class FromClause {
    public void parseFrom(String[] tableNames, StorageManager SM, Catalog catalog) {


        //Integer[] tableIds = new Integer[tableNames.length];
        ArrayList<Integer> tableIds = new ArrayList<>();
        for (String tName : tableNames) {
            tableIds.add(catalog.getTable(tName));
        }

        for (Integer Id : tableIds) {
            Table table = new Table(SM, Id);
            // Selects all 
            table.findMatching(r -> true, r -> System.out.println(r.data));
        }
    }
}
