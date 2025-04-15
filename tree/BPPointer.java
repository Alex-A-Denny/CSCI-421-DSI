package tree;

import java.nio.ByteBuffer;

public final class BPPointer {

    public final int pageNum;
    public final int entryNum;

    public static BPPointer node(int pageNum) {
        return new BPPointer(pageNum, -1);
    }

    public static BPPointer table(int pageNum, int entryNum) {
        return new BPPointer(pageNum, entryNum);
    }

    public static BPPointer nullPtr() {
        return new BPPointer(-1, -1);
    }

    public boolean isNull() {
        return pageNum == -1 && entryNum == -1;
    }

    public boolean isNode() {
        return entryNum == -1;
    }

    public boolean isTable() {
        return !isNull() && !isNode();
    }

    private BPPointer(int pageNum, int entryNum) {
        this.pageNum = pageNum;
        this.entryNum = entryNum;
    }

    public ByteBuffer encode() {
        ByteBuffer buf = ByteBuffer.allocate(Integer.BYTES * 2);
        buf.putInt(pageNum);
        buf.putInt(entryNum);
        buf.rewind();
        return buf;
    }

    public static BPPointer decode(ByteBuffer buf) {
        int pageNum = buf.getInt();
        int entryNum = buf.getInt();
        return new BPPointer(pageNum, entryNum);
    }

    @Override
    public String toString() {
        if (isNull()) {
            return "BPPointer{NULL}";
        }
        if (isNode()) {
            return "BPPointerNode{" + pageNum + "}";
        }
        return "BPPointerLeaf{" +
                "pageNum=" + pageNum +
                ", entryNum=" + entryNum +
                '}';
    }
}
