package page;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

// Author: Spencer Warren

public class Page {
    public final int id;
    public final ByteBuffer buf;

    public Page(int id, ByteBuffer buf) {
        this.id = id;
        this.buf = buf;
    }

    /**
     * Read the contents of the page
     *
     * @param codec the codec for the data
     * @return the data stored
     */
    public List<RecordEntry> read(RecordCodec codec) {
        return read(codec, Integer.MAX_VALUE);
    }

    /**
     * Read the contents of the page
     *
     * @param codec the codec for the data
     * @return the data stored
     */
    public List<RecordEntry> read(RecordCodec codec, int limit) {
        buf.rewind();
        int count = Math.min(buf.getInt(), limit);
        List<RecordEntry> list = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            list.add(codec.decode(buf));
        }
        buf.rewind();
        return list;
    }

    /**
     * Write the contents of the page
     * @param codec the codec for the data
     * @param list the list of entries to write
     * @param start the starting position within the list
     * @return the amount of entries written
     */
    public int write(RecordCodec codec, List<RecordEntry> list, int start) {
        if (list.isEmpty() || start >= list.size()) {
            return 0;
        }

        buf.position(4); // skip the record count
        int written = 0;
        for (int i = start; i < list.size(); i++) {
            ByteBuffer encoded = codec.encode(list.get(i));
            if (buf.position() + encoded.capacity() < buf.capacity()) {
                encoded.rewind();
                buf.put(encoded);
                written++;
            } else {
                buf.rewind();
                int currentAmount = buf.getInt();
                buf.rewind();
                buf.putInt(currentAmount + written);
                buf.rewind();
                return written;
            }
        }

        int pos = buf.position();
        buf.rewind();
        buf.putInt(written);
        buf.position(pos);
        return written;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Page page)) {
            return false;
        }
        return id == page.id;
    }

    @Override
    public int hashCode() {
        return id;
    }
}
