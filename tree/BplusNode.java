package tree;

import java.util.ArrayList;
import java.util.List;

public class BplusNode {

    public boolean isLeaf;
    public boolean isRoot;

    public int pageId;
    public int parentPageId;

    // The keys stored in this node.
    public List<Comparable<?>> keys;
    public List<Integer> children;

    public List<Integer> pagePointers; 
    public List<Integer> slotPointers;

    // The next leaf pointer, if not leaf then dont use
    public int nextLeafPageId;

    public BplusNode(boolean isLeaf, boolean isRoot, int pageId) {
        this.isLeaf = isLeaf;
        this.isRoot = isRoot;
        this.pageId = pageId;
        this.parentPageId = -1;
        this.keys = new ArrayList<>();
        this.children = new ArrayList<>();
        this.pagePointers = new ArrayList<>();
        this.slotPointers = new ArrayList<>();
        this.nextLeafPageId = -1;
    }

    @Override
    public String toString() {
        if (isLeaf) {
            return String.format(
                "LeafNode(page=%d, keys=%s, pagePtrs=%s, slotPtrs=%s, nextLeaf=%d, root=%s)",
                pageId, keys, pagePointers, slotPointers, nextLeafPageId, isRoot
            );
        } else {
            return String.format(
                "InternalNode(page=%d, keys=%s, children=%s, root=%s)",
                pageId, keys, children, isRoot
            );
        }
    }
}