package clauses;

import catalog.Catalog;
import page.RecordCodec;
import page.RecordEntry;
import page.RecordEntryType;
import storage.StorageManager;
import table.Table;
import table.TableSchema;

import java.util.ArrayList;
import java.util.List;

public class SelectClause {

    /**
     * Build a projected table containing only requested columns.
     *
     * @param sourceTable   The table returned by FromClause or after WHERE.
     * @param columnNames   Columns the user asked for in SELECT. Could be ["*"].
     * @param storageManager For allocating pages, etc.
     * @param catalog       Catalog for creating new tables.
     * @return the same table (if SELECT *) or new table with fewer columns.
     */
    public static Table parseSelect(
            Table sourceTable,
            String[] columnNames,
            StorageManager storageManager,
            Catalog catalog) 
    {
        // If SELECT *, just return the same table
        if (columnNames.length == 1 && columnNames[0].trim().equals("*")) {
            return sourceTable;
        }

        // Old schema
        TableSchema oldSchema = sourceTable.getSchema();
        List<String> oldColNames = oldSchema.names;
        List<RecordEntryType> oldTypes = oldSchema.types;
        List<Integer> oldSizes = oldSchema.sizes;

        // new schema lists
        List<String> newNames       = new ArrayList<>();
        List<RecordEntryType> newTypes     = new ArrayList<>();
        List<Integer> newSizes      = new ArrayList<>();
        List<Object> newDefaults    = new ArrayList<>();
        List<Boolean> newUniques    = new ArrayList<>();
        List<Boolean> newNullables  = new ArrayList<>();

        // track which old-schema column indices to copy
        List<Integer> colIndexes = new ArrayList<>(columnNames.length);

        // For each requested column, find the index in old schema and copy metadata
        for (String requestedCol : columnNames) {
            String colTrim = requestedCol.trim();
            int index = oldColNames.indexOf(colTrim);
            if (index == -1) {
                throw new RuntimeException("SELECT ERROR: Column not found: " + colTrim);
            }

            newNames.add(oldColNames.get(index));
            newTypes.add(oldTypes.get(index));
            newSizes.add(oldSizes.get(index));
            newDefaults.add(oldSchema.defaultValues.get(index));
            // TableSchema calls 'uniques' and 'nullables 
            newUniques.add(oldSchema.uniques.get(index));
            newNullables.add(oldSchema.nullables.get(index));

            colIndexes.add(index);
        }

        // Ffigure out the new primary key index
        int oldPKIndex = oldSchema.primaryKeyIndex;
        int newPKIndex = -1;
        if (oldPKIndex >= 0) {
            // is the old PK included among the selected columns
            int includedPos = colIndexes.indexOf(oldPKIndex);
            if (includedPos != -1) {
                newPKIndex = includedPos;
            }
        }

        // Create the new schema
        TableSchema newSchema = new TableSchema(
            newNames,
            newTypes,
            newSizes,
            newDefaults,
            newUniques,
            newNullables,
            newPKIndex,
            false // no re-compute sizes
        );

        // Create new table in catalog forprojected columns
        String newTableName = sourceTable.getName() + "_proj_" + System.currentTimeMillis();
        int newTableId = catalog.createTable(newTableName, new RecordCodec(newSchema));
        Table projectedTable = new Table(storageManager, newTableId);

        // Copy each row from source table, selectvonly requested columns
        sourceTable.findMatching(
            row -> true, // match everything
            row -> {
                List<Object> oldValues = row.data;
                List<Object> newValues = new ArrayList<>(colIndexes.size());
                for (int colIndex : colIndexes) {
                    newValues.add(oldValues.get(colIndex));
                }
                RecordEntry newRecord = new RecordEntry(newValues);
                // falso so we dont flush disk inmediatly
                projectedTable.insert(newRecord, false);
            }
        );

        return projectedTable;
    }
}