package tree;

import java.util.ArrayList;
import java.util.List;

public class BplusLeafNode extends BplusNode {
    // where record is in the table. 
    private final List<Integer> pagePointers;  
    private final List<Integer> slotPointers;

    // Pointer to next leaf page, or -1 if none
    private int nextLeafPageId;

    public BplusLeafNode(int pageId, int maxKeys) {
        super(true, pageId, maxKeys);
        this.pagePointers = new ArrayList<>();
        this.slotPointers = new ArrayList<>();
        this.nextLeafPageId = -1;
    }

    public int getNextLeafPageId() {
        return nextLeafPageId;
    }

    public void setNextLeafPageId(int pid) {
        nextLeafPageId = pid;
    }

    public List<Integer> getPagePointers() {
        return pagePointers;
    }

    public List<Integer> getSlotPointers() {
        return slotPointers;
    }

    public void insertEntry(Comparable<?> key, int pagePtr, int slotPtr) {
        // todo
    }

    public boolean removeEntry(Comparable<?> key) {
        //todo
    }

    @Override
    public boolean isOverCapacity() {
        return this.keys.size() > maxKeys;
    }

    @Override
    public boolean isUnderCapacity() {
        // im asuming we need at least 2 keys after mergers like in class
        return this.keys.size() < (2);
    }
}
