// Author: Sam Ellis

import java.nio.ByteBuffer;
import java.nio.IntBuffer;
import java.util.ArrayList;
import page.RecordEntryType;

public class Schema { // Schema within a particular table
    private Integer pageSize;                     // Max size of a page in bytes
    private ArrayList<Integer> pageIDs;           // List of IDs for all pages, in order
    private ArrayList<RecordEntryType> attributeTypes; // List of attribute types, in order
    private Integer pageIDCount = 0;              // Internal counter, used for assigning IDs to new pages

    public Schema(Integer pageSize, ArrayList<Integer> pageIDs, ArrayList<RecordEntryType> attributeTypes) {
        this.pageSize = pageSize;
        this.pageIDs = pageIDs;
        this.attributeTypes = attributeTypes;
    }

    public Schema(byte[] byteSchema) {

        IntBuffer ibuf = ByteBuffer.wrap(byteSchema).asIntBuffer();

        int pageSize = ibuf.get();
        this.pageSize = pageSize;

        int numPageIDs = ibuf.get();
        ArrayList<Integer> pageIDs = new ArrayList<>();
        for (int i=0; i<numPageIDs; i++) {
            pageIDs.add(ibuf.get());
        }
        this.pageIDs = pageIDs;

        int numAttrTypes = ibuf.get();
        ArrayList<RecordEntryType> attributeTypes = new ArrayList<RecordEntryType>();
        for (int i=0; i<numAttrTypes; i++) {
            int ordinal = ibuf.get();
            switch (ordinal) {
                case 0 -> attributeTypes.add(RecordEntryType.INT);
                case 1 -> attributeTypes.add(RecordEntryType.DOUBLE);
                case 2 -> attributeTypes.add(RecordEntryType.BOOL);
                case 3 -> attributeTypes.add(RecordEntryType.CHAR_FIXED); // I dont think this stores string length
                case 4 -> attributeTypes.add(RecordEntryType.CHAR_VAR);   // 
            }
        }

        int pageIDCount = ibuf.get();
        this.pageIDCount = pageIDCount;
    }

    public void addPage() {
        pageIDs.add(pageIDCount); //TODO: needs proper logic for ordering
        pageIDCount += 1;
    }

    public void removePage(Integer pageID) {
        pageIDs.remove(pageID); //TODO: needs proper logic for ordering
    }

    public ArrayList<Integer> getPageIDs() {
        return pageIDs;
    }

    public ArrayList<RecordEntryType> getAttributeTypes() {
        return attributeTypes;
    }

    public byte[] toBytes() {
        int totalBytes = (RecordEntryType.INT.size() * 4) +   // pageSize, pageIDCount, pageIDs size, attrTypes size
                         (RecordEntryType.INT.size() * pageIDs.size()) + 
                         (RecordEntryType.INT.size() * attributeTypes.size());
        ByteBuffer bbuf = ByteBuffer.allocate(totalBytes); // Allocate buffer
        
        bbuf.putInt(pageSize);       // Store pageSize
        bbuf.putInt(pageIDs.size()); // #of pageIDs
        for (Integer id : pageIDs) { // Store pageIDs
            bbuf.putInt(id);
        }
        bbuf.putInt(attributeTypes.size());  // #of attrtypes
        for (RecordEntryType type : attributeTypes) {
            bbuf.putInt(type.ordinal()); // Store attributeTypes
        }
        bbuf.putInt(pageIDCount);   // Store pageIDCount

        return bbuf.array();
    }

    public void printSchema() {
        for (RecordEntryType e : attributeTypes) {
            System.out.print(e.name());
        }
    }
}