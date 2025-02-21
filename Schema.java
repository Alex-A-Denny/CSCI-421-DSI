// Author: Sam Ellis

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import page.RecordEntryType;

public class Schema { // Schema within a particular table
    private Integer pageSize;                     // Max size of a page in bytes
    private ArrayList<Integer> pageIDs;           // List of IDs for all pages, in order
    private List<RecordEntryType> attributeTypes; // List of attribute types, in order
    private Integer pageIDCount = 0;              // Internal counter, used for assigning IDs to new pages

    public Schema(Integer pageSize, ArrayList<Integer> pageIDs, List<RecordEntryType> attributeTypes) {
        this.pageSize = pageSize;
        this.pageIDs = pageIDs;
        this.attributeTypes = attributeTypes;
    }

    public Schema(byte[] byteSchema) {
        // TODO: Initalize schema object from encoded byte array from .toBytes()
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

    public List<RecordEntryType> getAttributeTypes() {
        return attributeTypes;
    }

    public byte[] toBytes() {
        int totalBytes = (RecordEntryType.INT.size() * 4) +   // pageSize, pageCount, pageIDs size, attrTypes size
                         (RecordEntryType.INT.size() * pageIDs.size()) + 
                         (RecordEntryType.INT.size() * attributeTypes.size());
        ByteBuffer bbuf = ByteBuffer.allocate(totalBytes); // Allocate buffer
        
        bbuf.putInt(pageSize);       // Store pageSize
        bbuf.putInt(pageIDs.size()); 
        for (Integer id : pageIDs) { // Store pageIDs
            bbuf.putInt(id);
        }
        bbuf.putInt(attributeTypes.size()); 
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