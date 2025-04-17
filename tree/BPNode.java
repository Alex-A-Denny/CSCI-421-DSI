package tree;

import page.Page;
import page.RecordEntryType;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

public class BPNode {

    public final int tableId;
    public final int pageNum;
    public final List<Object> values;
    public final List<BPPointer> pointers;
    public final RecordEntryType entryType;
    public boolean isLeaf;

    public BPNode(int tableId, int pageNum, List<Object> values, List<BPPointer> pointers, RecordEntryType entryType, boolean isLeaf) {
        this.tableId = tableId;
        this.pageNum = pageNum;
        this.values = values;
        this.pointers = pointers;
        this.entryType = entryType;
        this.isLeaf = isLeaf;
    }

    /**
     * @param value the value to compare against
     * @return the index of the first value which is <= the provided value
     */
    public int findLEq(Object value) {
        for (int i = 0; i < values.size(); i++) {
            if (compare(value, values.get(i)) <= 0) {
                return i;
            }
        }
        return -1;
    }

    public boolean isInternal() {
        if (pointers.getFirst().isNull()) {
            if (pointers.size() > 1) {
                return pointers.get(1).isNode();
            }
            return true;
        }
        return pointers.getFirst().isNode();
    }

    public void print() {
        System.out.println("PageNum: " + pageNum);
        System.out.println("Values: " + values);
        System.out.println("Pointers: " + pointers);
        System.out.println("Leaf: " + isLeaf);
        System.out.println();
    }

    public void save() {
        Page page = BPTree.storageManager.getIndexPage(tableId, pageNum);
        page.buf.rewind();
        page.buf.clear();
        page.buf.rewind();
        page.buf.put((byte) (isLeaf ? 1 : 0));
        page.buf.putInt(values.size());
        for (Object value : values) {
            if (value instanceof Integer i) {
                page.buf.putInt(i);
            } else if (value instanceof Double d) {
                page.buf.putDouble(d);
            } else if (value instanceof Boolean b) {
                page.buf.put((byte) (b ? 1 : 0));
            } else if (value instanceof String s) {
                byte[] arr = s.getBytes(StandardCharsets.UTF_8);
                page.buf.putInt(arr.length);
                page.buf.put(arr);
            }
        }
        page.buf.putInt(pointers.size());
        for (BPPointer pointer : pointers) {
            page.buf.put(pointer.encode());
        }
        page.buf.rewind();
    }

    public static BPNode get(int tableId, int pageNum, RecordEntryType entryType) {
        Page page = BPTree.storageManager.getIndexPage(tableId, pageNum);
        page.buf.rewind();
        boolean isLeaf = page.buf.get() == 1;
        int valueSize = page.buf.getInt();
        List<Object> values = new ArrayList<>(valueSize);
        for (int i = 0; i < valueSize; i++) {
            Object value = switch (entryType) {
                case INT -> page.buf.getInt();
                case DOUBLE -> page.buf.getDouble();
                case BOOL -> page.buf.get() == 1;
                case CHAR_FIXED, CHAR_VAR -> {
                    int length = page.buf.getInt();
                    byte[] bytes = new byte[length];
                    page.buf.get(bytes);
                    yield new String(bytes, StandardCharsets.UTF_8);
                }
            };
            values.add(value);
        }
        int pointerSize = page.buf.getInt();
        List<BPPointer> pointers = new ArrayList<>(pointerSize);
        for (int i = 0; i < pointerSize; i++) {
            pointers.add(BPPointer.decode(page.buf));
        }
        return new BPNode(tableId, pageNum, values, pointers, entryType, isLeaf);
    }

    private static int compare(Object a, Object b) {
        if (a instanceof Integer i) {
            return i.compareTo((Integer) b);
        }
        if (a instanceof Double d) {
            return d.compareTo((Double) b);
        }
        if (a instanceof Boolean bool) {
            return bool.compareTo((Boolean) b);
        }
        if (a instanceof String s) {
            return s.compareTo((String) b);
        }
        throw new IllegalArgumentException("Comparison arguments were of invalid types");
    }
}
