package page;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import table.TableSchema;

// Author: Spencer Warren

public class RecordCodec {
    public final TableSchema schema;

    /**
     * @param schema the schema of the table this codec applies to
     */
    public RecordCodec(TableSchema schema) {
        this.schema = schema;
    }

    /**
     * @param entry the entry to encode
     * @return the encoded form
     */
    public ByteBuffer encode(RecordEntry entry) {
        List<Object> list = entry.data;
        if (list.size() != schema.types.size()) {
            throw new IllegalArgumentException("Codec input list size does not match: " + list);
        }

        int size = 0;
        int mask = 0;
        for (int i = 0; i < list.size(); i++) {
            Object o = list.get(i);
            RecordEntryType type = schema.types.get(i);

            if (o == null) {
                if (schema.nullables.get(i)) {
                    mask |= (1 << i);
                } else {
                    throw new IllegalArgumentException("Value is not allowed to be null at index: " + i);
                }
            } else if (type.matchesType(o)) {
                if (type == RecordEntryType.CHAR_VAR) {
                    size += RecordEntryType.INT.size();
                    size += ((String) o).getBytes().length;
                } else {
                    size += schema.sizes.get(i);
                }
            } else {
                throw new IllegalArgumentException("Value at index " + i + " is of incorret type");
            }
        }

        size += RecordEntryType.INT.size(); // for the null bitmask

        ByteBuffer buf = ByteBuffer.allocate(size);
        buf.putInt(mask);

        for (int i = 0; i < list.size(); i++) {
            Object o = list.get(i);
            if (o == null) {
                continue;
            } else if (o instanceof Integer j) {
                buf.putInt(j);
            } else if (o instanceof Double d) {
                buf.putDouble(d);
            } else if (o instanceof Boolean b) {
                buf.put((byte) (b ? 1 : 0));
            } else if (o instanceof String s) {
                RecordEntryType type = schema.types.get(i);
                if (type == RecordEntryType.CHAR_FIXED) {
                    byte[] arr = s.getBytes();
                    // ensure the array is the entire size for fixed length
                    arr = Arrays.copyOf(arr, schema.sizes.get(i));
                    buf.put(arr);
                } else if (type == RecordEntryType.CHAR_VAR) {
                    byte[] arr = s.getBytes();
                    buf.putInt(arr.length);
                    buf.put(arr);
                }
            } else {
                throw new IllegalStateException("Cannot encode value: " + o);
            }
        }

        if (buf.position() != size) {
            throw new IllegalStateException("Unable to fully encode record to buffer, pos=" + buf.position() + ", size=" + size);
        }

        buf.rewind();
        return buf;
    }

    /**
     * @param buf the encoded form, with position at the start of the region to read
     * @return the decoded form
     */
    public RecordEntry decode(ByteBuffer buf) {
        List<Object> list = new ArrayList<>();
        int mask = buf.getInt();
        for (int i = 0; i < schema.types.size(); i++) {
            RecordEntryType type = schema.types.get(i);
            int maskIndex = 1 << i;
            boolean isNull = (mask & maskIndex) != 0;
            if (isNull) {
                if (schema.nullables.get(i)) {
                    list.add(null);
                } else {
                    throw new IllegalArgumentException("Value cannot be null at index:" + i);
                }
            } else {
                switch (type) {
                    case INT -> list.add(buf.getInt());
                    case DOUBLE -> list.add(buf.getDouble());
                    case BOOL -> list.add(buf.get() == 1);
                    case CHAR_FIXED -> {
                        int size = schema.sizes.get(i);
                        byte[] arr = new byte[size];
                        buf.get(arr);

                        // determine where padding starts
                        int paddingStart = -1;
                        for (int j = arr.length - 1; j >= 0; j--) {
                            if (arr[j] != 0) {
                                paddingStart = j + 1;
                                break;
                            }
                        }
                        if (paddingStart < 0) {
                            paddingStart = arr.length;
                        }

                        // remove trailing null bytes
                        byte[] trimmed = new byte[paddingStart];
                        System.arraycopy(arr, 0, trimmed, 0, paddingStart);
                        list.add(new String(trimmed));
                    }
                    case CHAR_VAR -> {
                        int count = buf.getInt();
                        byte[] arr = new byte[count];
                        buf.get(arr);
                        list.add(new String(arr));
                    }
                }
            }
        }

        return new RecordEntry(list);
    }

    public int compareRecords(RecordEntry e1, RecordEntry e2) {
        Object key1 = e1.data.get(schema.primaryKeyIndex);
        Object key2 = e2.data.get(schema.primaryKeyIndex);
        if (key1 instanceof Integer i) {
            return i.compareTo((Integer) key2);
        } else if (key1 instanceof Double d) {
            return d.compareTo((Double) key2);
        } else if (key1 instanceof Boolean b) {
            return b.compareTo((Boolean) key2);
        } else if (key1 instanceof String s) {
            return s.compareTo((String) key2);
        }
        throw new IllegalStateException("Record primary key had disallowed type");
    }
}
