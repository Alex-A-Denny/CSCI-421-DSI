package tree;

import java.util.ArrayList;
import java.util.List;

public class BplusInternalNode extends BplusNode {

    // Children[i] is the pageId of the i-th child node.
    private final List<Integer> children;

    public BplusInternalNode(int pageId, int maxKeys) {
        super(false, pageId, maxKeys);
        this.children = new ArrayList<>();
    }

    public List<Integer> getChildren() {
        return children;
    }

    // Insert a key and child pointer
    public void insertChild(int idx, Comparable<?> key, int childRight) {
        // todo
    }

    public void removeKey(int idx) {
        // todo
    }

    @Override
    public boolean isOverCapacity() {
        return this.keys.size() > maxKeys;
    }

    @Override
    public boolean isUnderCapacity() {
        return this.keys.size() < (2);
    }
}
}
