package tree;

public class Bplus {
    /**
     * Calculate 'N' of the tree, the number of pointers in a node.
     * @param maxPageSize number of bytes per page
     * @param maxKeySize max number of bytes per primary key to be indexed
     * @return
     */
    private int rootPageId;
    private final int maxKeys;


    public Integer calculateN(Integer maxPageSize, Integer maxKeySize) {
        //                                   Add integer for size of pointer
        return ((int) Math.floor(maxPageSize / (maxKeySize + Integer.SIZE))) - 1;
    }
}
