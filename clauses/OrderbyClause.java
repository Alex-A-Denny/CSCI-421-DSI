// Author: Sam Ellis | sae1200@rit.edu

package clauses;

import catalog.Catalog;
import storage.StorageManager;
import page.RecordCodec;
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
        if (columnName == null) {
            return table;
        }
        int newPrimaryKeyIndex = table.getSchema().getColumnIndex(columnName);
        if (newPrimaryKeyIndex < 0) {
            System.err.println("Error: unable to find column " + columnName);
            return null;
        }
        TableSchema newSchema = table.getSchema().copy(newPrimaryKeyIndex);
        int id = catalog.createTable("Ordered[" + table.getName() + "]", new RecordCodec(newSchema));
        Table ordered = new Table(SM, id);
        table.findMatching(r -> true, r -> {
            ordered.insert(r, false);
        });

        return ordered; // Make sure to cleanup!
    }
}
