// Author: Sam Ellis | sae1200@rit.edu

package clauses;

import java.util.ArrayList;
import java.util.List;

import catalog.Catalog;
import storage.StorageManager;
import page.Page;
import page.RecordCodec;
import page.RecordEntry;
import table.Table;
import table.TableSchema;

public class OrderbyClause {

    /**
     * Returns a new table, with the same records as the original, but sorted on a provided column.
     * The new table must be deleted after use. 
     * 
     * @param table original table to be sorted
     * @param columnName name of column to sort new table on
     * @param SM Predefined storage manager
     * @param catalog Predefined catalog
     */
    public static Table parseOrderby(Table table, String columnName, StorageManager SM, Catalog catalog) {

        if (columnName == null) return table;
        
        // Get table name, ID, schema
        String tName = table.getName();
        Integer oldTableID = catalog.getTable(tName);
        RecordCodec oldCodec = catalog.getCodec(oldTableID);

        // Establish new codec 
        Integer newPrimaryKeyIndex = table.getSchema().getColumnIndex(columnName);
        TableSchema newSchema = oldCodec.schema.copy(newPrimaryKeyIndex);
        RecordCodec newCodec = new RecordCodec(newSchema);

        // Add the table to the catalog
        catalog.createTable("ordered[" + tName + "]", newCodec);
        Integer newTableID = catalog.getTable("ordered[" + tName + "]");
        Table newTable = new Table(SM, newTableID);
        
        // Insert values into new table
        List<Integer> pageIDs = new ArrayList<>(catalog.getPages(oldTableID));
        for (int pageNum : pageIDs) {

            Page curPage = SM.getPage(oldTableID, pageNum);
            List<RecordEntry> pageEntries = curPage.read(oldCodec);
            for (var entry : pageEntries) {

                List<Object> entryData = new ArrayList<>(entry.data);
                newTable.insert(new RecordEntry(entryData), false);
            }
        }

        return newTable; // Make sure to cleanup!
    }
}
