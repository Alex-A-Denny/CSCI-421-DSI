package tree;

import java.util.ArrayList;
import java.util.List;

public class BplusNode {
    private final boolean isLeaf; // is node leaf
    private int pageId;
    private int parentPageId; // parent node's page ID, -1 if none

    // list of keys in this node. 
    private final List<Integer> keys;
    // max keys (order) of the tree. zaazaa
    private final int maxKeys;

    public boolean isLeaf() {
        return this.isLeaf;
    }

    public int getPageId() {
        return pageId;
    }

    public int getParentPageId() {
        return parentPageId;
    }

}
