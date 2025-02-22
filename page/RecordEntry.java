package page;

import java.util.List;

// Author: Spencer Warren

public class RecordEntry {

    public final List<Object> data;

    public RecordEntry(List<Object> data) {
        this.data = data;
    }

    @Override
    public String toString() {
        return data.toString();
    }
}