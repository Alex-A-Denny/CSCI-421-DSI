import java.io.Serializable;
import java.util.List;

public class Schema implements Serializable {
    private List<String> attributeTypes; // List of attribute types in order

    public Schema(List<String> attributeTypes) {
        this.attributeTypes = attributeTypes;
    }

    public List<String> getAttributeTypes() {
        return attributeTypes;
    }

    public void printSchema() {
        System.out.println("Schema: " + String.join(", ", attributeTypes));
    }
}