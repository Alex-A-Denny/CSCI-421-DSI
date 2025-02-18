package page;

import java.nio.ByteBuffer;

public class Page {
    public final int id;
    public final ByteBuffer buf;

    public Page(int id, ByteBuffer buf) {
        this.id = id;
        this.buf = buf;
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
