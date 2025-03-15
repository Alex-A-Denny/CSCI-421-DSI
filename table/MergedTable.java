package table;

import page.RecordEntry;

// Author: Spencer Warren

public class MergedTable extends VirtualTable {

    public MergedTable(TableSchema schema, String name) {
        super(schema, name);
    }

    @Override
    protected boolean checkConstraints(RecordEntry entry) {
        // ignore all constraints
        return true;
    }
}
