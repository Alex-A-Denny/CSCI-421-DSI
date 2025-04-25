package tree;

import catalog.Catalog;
import page.RecordEntryType;
import storage.StorageManager;
import table.TableSchema;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;

public class BPTree {

    public static Catalog catalog;
    public static StorageManager storageManager;

    private final int tableId;
    private final RecordEntryType entryType;
    private final int maxPointers;

    public static int nValue;

    private BPPointer root = null;

    public BPTree(int tableId, RecordEntryType entryType, int maxPointers) {
        this.tableId = tableId;
        this.entryType = entryType;
        this.maxPointers = maxPointers;
        
        TableSchema schema = catalog.getCodec(tableId).schema;
        this.nValue = (int) Math.floor((catalog.getPageSize() / (schema.sizes.get(schema.primaryKeyIndex) + 4))) - 1;//N-value of B+ Tree
    }                                                                                        //Alex Denny

    public void print() {
        if (root == null) {
            System.out.println("No Root");
            return;
        } else {
            System.out.println("Root");
        }
        var node = BPNode.get(tableId, root.pageNum, entryType);
        print(node);
    }

    private void print(BPNode node) {
        node.print();
        if (node.isLeaf) {
            return;
        }

        for (BPPointer pointer : node.pointers) {
            if (pointer.isNull() || pointer.isTable()) {
                continue;
            }
            var childNode = BPNode.get(tableId, pointer.pageNum, entryType);
            print(childNode);
        }
    }

    public BPPointer search(Object valueToFind) {
        if (root == null) {
            root = BPPointer.node(catalog.getIndexHead(tableId));
        }
        BPNode node = getNode(root.pageNum);
        while (node.isInternal()) {
            int index = node.findLEq(valueToFind);
            BPPointer pointer;
            if (index < 0) {
                // the value was not <= anything, so take the last node
                pointer = node.pointers.getLast();
            } else {
                if (node.values.get(index).equals(valueToFind)) {
                    // are equal, use the index as-is
                    pointer = node.pointers.get(index);
                } else {
                    // must be <, so get the pointer before it
                    pointer = node.pointers.get(Math.max(0, index - 1));
                }
            }
            node = getNode(pointer.pageNum);
        }

        int index = node.values.indexOf(valueToFind);
        if (index < 0) {
            // not found

            // hack to get around search keys not actually having all values strictly <= to them on the left
            // due to how node splitting works
            BPPointer pointer = node.pointers.getLast();
            if (pointer.isNull()) {
                index = node.findLEq(valueToFind);
                return node.pointers.get(index);
            }
            if (pointer.isNode()) {
                BPNode nextNode = getNode(pointer.pageNum);
                index = nextNode.values.indexOf(valueToFind);
                if (index < 0) {
                    index = node.findLEq(valueToFind);
                    if (index < 0) {
                        return node.pointers.get(node.pointers.size() - 2);
                    }
                    if (node.pointers.get(index).isNode()) {
                        node = getNode(node.pointers.get(index).pageNum);
                        return node.pointers.getFirst();
                    }
                    return node.pointers.get(index);
                }
                node = nextNode;
            } else {
                index = node.findLEq(valueToFind);
                if (index < 0) {
                    index = node.values.size() - 1;
                }
            }
        }
        BPPointer pointer = node.pointers.get(index);
        if (pointer.isNull()) {
            if (index + 1 < node.pointers.size()) {
                pointer = node.pointers.get(index + 1);
            } else {
                pointer = node.pointers.get(node.pointers.size() - 2);
            }
        }
        if (pointer.isNode()) {
            // found a pointer to the right, get the first value there instead
            node = getNode(pointer.pageNum);
            return node.pointers.getFirst();
        }
        return pointer;
    }

    public boolean insert(Object valueToInsert, BPPointer ptrToInsert) {
        if (ptrToInsert.isNull() || ptrToInsert.isNode()) {
            throw new IllegalArgumentException("Pointer cannot be null or node type");
        }
        if (root == null) {
            int pageNum = catalog.requestNewIndexPageNum();
            catalog.setIndexHead(tableId, pageNum);
            root = BPPointer.node(pageNum);
            BPNode node = new BPNode(tableId, pageNum, new ArrayList<>(), new ArrayList<>(), entryType, true);
            node.values.add(valueToInsert);
            node.pointers.add(BPPointer.nullPtr());
            node.pointers.add(ptrToInsert);
            node.save();
            return true;
        }

        BPPointer pointer = root;
        BPNode node = getNode(pointer.pageNum);
        ArrayDeque<BPPointer> queue = new ArrayDeque<>();
        while (node.isInternal()) {
            queue.add(pointer);
            int index = node.findLEq(valueToInsert);
            if (index < 0) {
                // the value was not <= anything, so take the last node
                pointer = node.pointers.getLast();
            } else {
                Object existing = node.values.get(index);
                if (valueToInsert.equals(existing)) {
                    // conflict
                    return false;
                }

                pointer = node.pointers.get(index);
            }
            node = getNode(pointer.pageNum);
        }
        queue.add(pointer);

        BPNode targetNode = node;
        int index = targetNode.findLEq(valueToInsert);
        if (index < 0) {
            // value was >= everything, insert it at the end
            targetNode.values.addLast(valueToInsert);
            insertPointer(targetNode, ptrToInsert);
        } else {
            if (targetNode.values.get(index).equals(valueToInsert)) {
                // conflict
                return false;
            }

            targetNode.values.add(index, valueToInsert);
            if (index == 0 && targetNode.pointers.getFirst().isNull()) {
                // special case where the bottom left of the tree contains a null pointer
                // when nothing is less than it
                targetNode.pointers.set(0, ptrToInsert);
            } else {
                targetNode.pointers.add(index, ptrToInsert);
            }
        }
        splitRoutine(queue, pointer, targetNode);
        targetNode.save();
        return true;
    }

    private void splitRoutine(ArrayDeque<BPPointer> pointers, BPPointer firstPointer, BPNode firstNode) {
        BPPointer pointer;
        BPNode node;
        if (firstPointer == null) {
            pointer = pointers.removeLast();
            node = getNode(pointer.pageNum);
        } else {
            pointer = firstPointer;
            node = firstNode;
            pointers.removeLast();
        }

        if (node.pointers.size() <= maxPointers) {
            return;
        }

        int half = node.values.size() / 2;
        List<Object> newValues = new ArrayList<>(node.values.size() - half);
        for (int i = half; i < node.values.size(); i++) {
            newValues.add(node.values.get(i));
        }
        for (int i = 0; i < newValues.size(); i++) {
            node.values.removeLast();
        }

        half = node.pointers.size() / 2;
        List<BPPointer> newPointers = new ArrayList<>(node.pointers.size() - half);
        for (int i = half; i < node.pointers.size(); i++) {
            newPointers.add(node.pointers.get(i));
        }
        for (int i = 0; i < newPointers.size(); i++) {
            node.pointers.removeLast();
        }

        int newNodePageNum = catalog.requestNewIndexPageNum();
        // pointer to the right
        node.values.add(newValues.getFirst());
        node.pointers.add(BPPointer.node(newNodePageNum));
        node.save();

        BPNode newNode = new BPNode(tableId, newNodePageNum, newValues, newPointers, entryType, node.isLeaf);
        newNode.save();

        // update parent
        if (pointers.isEmpty()) {
            // the root was split
            int newRootPageNum = catalog.requestNewIndexPageNum();
            root = BPPointer.node(newRootPageNum);
            List<Object> rootValues = new ArrayList<>(1);
            rootValues.add(newNode.values.getFirst());
            List<BPPointer> rootPtrs = new ArrayList<>(2);
            rootPtrs.add(pointer);
            rootPtrs.add(BPPointer.node(newNodePageNum));
            BPNode newRoot = new BPNode(tableId, newRootPageNum, rootValues, rootPtrs, entryType, false);
            newRoot.save();
        } else {
            BPPointer parent = pointers.getLast();
            BPNode parentNode = getNode(parent.pageNum);
            int index = parentNode.findLEq(newNode.values.getFirst());
            if (index < 0) {
                parentNode.values.addLast(newNode.values.getFirst());
                insertPointer(parentNode, BPPointer.node(newNodePageNum));
            } else {
                parentNode.values.add(index, newNode.values.getFirst());
                parentNode.pointers.add(BPPointer.node(newNodePageNum));
            }
            if (parentNode.pointers.size() <= maxPointers) {
                parentNode.save();
            }

            if (!pointers.isEmpty()) {
                splitRoutine(pointers, BPPointer.node(parent.pageNum), parentNode);
            }
        }
    }

    private void insertPointer(BPNode parentNode, BPPointer newPointer) {
        BPPointer lastPointer = parentNode.pointers.getLast();
        if (parentNode.isLeaf && (lastPointer.isNode() || lastPointer.isNull())) {
            // insert BEFORE the pointer to the next leaf / null pointer
            parentNode.pointers.add(parentNode.pointers.size() - 2, newPointer);
        } else {
            parentNode.pointers.addLast(newPointer);
        }
    }

    public void update(Object start, Predicate<BPPointer> predicate, UnaryOperator<BPPointer> operator) {
        BPNode node = searchNode(start);
        if (node == null) {
            throw new IllegalArgumentException("Could not find node containing pointer to start");
        }
        boolean first = true;
        int startPos = node.values.indexOf(start) + 1;
        if (node.pointers.get(0).isNull()) {
            startPos++;
        }
        outer:
        while (true) {
            for (int i = startPos; i < node.pointers.size(); i++) {
                if (first) {
                    first = false;
                    startPos = 0;
                }
                var ptr = node.pointers.get(i);
                if (ptr.isNull()) {
                    continue;
                }
                if (ptr.isNode()) {
                    node = getNode(ptr.pageNum);
                    continue outer;
                }
                if (ptr.isTable() && predicate.test(ptr)) {
                    node.pointers.set(i, operator.apply(ptr));
                }
            }
            return;
        }
    }

    private BPNode searchNode(Object valueToFind) {
        if (root == null) {
            root = BPPointer.node(catalog.getIndexHead(tableId));
        }
        BPNode node = getNode(root.pageNum);
        while (node.isInternal()) {
            int index = node.findLEq(valueToFind);
            BPPointer pointer;
            if (index < 0) {
                // the value was not <= anything, so take the last node
                pointer = node.pointers.getLast();
            } else {
                if (node.values.get(index).equals(valueToFind)) {
                    // are equal, use the index as-is
                    pointer = node.pointers.get(index);
                } else {
                    // must be <, so get the pointer before it
                    pointer = node.pointers.get(Math.max(0, index - 1));
                }
            }
            node = getNode(pointer.pageNum);
        }

        int index = node.values.indexOf(valueToFind);
        if (index < 0) {
            // not found

            // hack to get around search keys not actually having all values strictly <= to them on the left
            // due to how node splitting works
            BPPointer pointer = node.pointers.getLast();
            if (pointer.isNull()) {
                index = node.findLEq(valueToFind);
                return node;
            }
            if (pointer.isNode()) {
                BPNode nextNode = getNode(pointer.pageNum);
                index = nextNode.values.indexOf(valueToFind);
                if (index < 0) {
                    return node;
                }
                node = nextNode;
            } else {
                index = node.findLEq(valueToFind);
                if (index < 0) {
                    index = node.values.size();
                }
            }
        }

        BPPointer pointer;
        if (index + 1 < node.pointers.size()) {
            pointer = node.pointers.get(index + 1);
        } else {
            pointer = node.pointers.get(index);
        }
        if (pointer.isNode()) {
            // found a pointer to the right, get the first value there instead
            return getNode(pointer.pageNum);
        }
        return node;
    }

    public boolean drop() {
        return storageManager.deleteIndex(tableId);
    }

    private void gatherAllPointers(List<BPPointer> list, BPPointer nodePtr) {
        BPNode node = getNode(nodePtr.pageNum);
        list.add(nodePtr);
        for (BPPointer ptr : node.pointers) {
            if (ptr.isNull() || ptr.isTable()) {
                continue;
            }
            list.add(ptr);
            if (ptr.isNode()) {
                gatherAllPointers(list, ptr);
            }
        }
    }

    private BPNode getNode(int pageNum) {
        return BPNode.get(tableId, pageNum, entryType);
    }
}
