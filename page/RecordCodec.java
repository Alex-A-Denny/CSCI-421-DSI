package page;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class RecordCodec {
    public final RecordEntryType[] types;
    public final int[] sizes;

    /**
     * 
     * @param types the data types for the codec
     * @param sizes the length of each value, use values < 0 for non string types
     */
    public RecordCodec(RecordEntryType[] types, int[] sizes) {
        this.types = types;
        this.sizes = sizes;
        if (types.length != sizes.length) {
            throw new IllegalArgumentException("length of types does not match length of sizes");
        }
        if (types.length > 32) {
            throw new IllegalArgumentException("cannot have more than 32 types");
        }
        for (int i = 0; i < sizes.length; i++) {
            if (sizes[i] < 0) {
                sizes[i] = types[i].size();
            } else {
                sizes[i] = sizes[i] * types[i].size();
            }
        }
    }

    /**
     * @param list the list of values to encode
     * @return the encoded form
     */
    public ByteBuffer encode(List<Object> list) {
        if (list.size() != types.length) {
            throw new IllegalArgumentException("Codec input list size does not match: " + list);
        }

        int size = 0;
        int mask = 0;
        for (int i = 0; i < list.size(); i++) {
            Object o = list.get(i);
            RecordEntryType type = types[i];

            if (o == null) {
                mask |= (1 << i);
            } else if (type.matchesType(o)) {
                if (type == RecordEntryType.CHAR_VAR) {
                    size += RecordEntryType.INT.size();
                    size += ((String) o).length() * RecordEntryType.CHAR_VAR.size();
                } else {
                    size += sizes[i];
                }
            } else {
                throw new IllegalArgumentException("Value at index " + i + " is of incorret type");
            }
        }

        size += RecordEntryType.INT.size();

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
                RecordEntryType type = types[i];
                if (type == RecordEntryType.CHAR_FIXED) {
                    byte[] arr = s.getBytes();
                    // ensure the array is the entire size for fixed length
                    arr = Arrays.copyOf(arr, sizes[i]);
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

        return buf;
    }

    /**
     * @param buf the encoded form, with position at the start of the region to read
     * @return the decoded form
     */
    public List<Object> decode(ByteBuffer buf) {
        List<Object> list = new ArrayList<>();
        int mask = buf.getInt();
        for (int i = 0; i < types.length; i++) {
            RecordEntryType type = types[i];
            int maskIndex = 1 << i;
            boolean isNull = (mask & maskIndex) != 0;
            if (isNull) {
                list.add(null);
            } else {
                switch (type) {
                    case INT -> list.add(buf.getInt());
                    case DOUBLE -> list.add(buf.getDouble());
                    case BOOL -> list.add(buf.get() == 1);
                    case CHAR_FIXED -> {
                        int size = sizes[i];
                        byte[] arr = new byte[size];
                        buf.get(arr);
                        list.add(new String(arr));
                    }
                    case CHAR_VAR -> {
                        int size = buf.getInt() * type.size();
                        byte[] arr = new byte[size];
                        buf.get(arr);
                        list.add(new String(arr));
                    }
                }
            }
        }

        if (buf.position() != buf.limit()) {
            throw new IllegalStateException("Unable to completely read buffer");
        }

        return list;
    }
}
