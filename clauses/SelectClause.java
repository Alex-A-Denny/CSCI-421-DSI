package clauses;

import table.Table;

public class SelectClause {

    /**
     * Build a projected table containing only requested columns.
     *
     * @param table the table to project
     * @param selectRaw the raw select clause
     */
    public static Table parseSelect(Table table, String selectRaw) {
        String[] columnNames = selectRaw.split(",");
        int[] columnIndices = new int[columnNames.length];
        for (int i = 0; i < columnNames.length; i++) {
            String column = columnNames[i].strip();
            if (column.equals("*")) {
                return table;
            }
            int index = table.getSchema().getColumnIndex(column);
            if (index >= 0) {
                columnIndices[i] = index;
            } else {
                System.err.println("Error: no such column exists: " + column);
                return null;
            }
        }

        return table.toSelected(columnIndices);
    }

}