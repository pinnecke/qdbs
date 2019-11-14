package ovgu.dbse.pinnecke.qdbs;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;

public class Schemas {

    public static void attributeListToFile(RandomAccessFile file, List<Schema.Attribute> attributes) throws IOException {
        file.writeInt(attributes.size());
        for (Schema.Attribute attr : attributes) {
            attr.writeToFile(file);
        }
    }

    public static List<Schema.Attribute> attributeListFromFile(RandomAccessFile file) throws IOException {
        int numAttributes = file.readInt();
        List<Schema.Attribute> result = new ArrayList<>(numAttributes);
        for (int i = 0; i < numAttributes; i++) {
            result.add(Schema.Attribute.readFromFile(file));
        }
        return result;
    }

    private static List<Schema.Attribute> prefixSchema(List<Schema.Attribute> source, String prefix) {
        List<Schema.Attribute> result = new ArrayList<>(source.size());
        for (Schema.Attribute a : source) {
            result.add(new Schema.Attribute(prefix + "." + a.getName(), a.getType()));
        }
        return result;
    }

    public static Schema combine(Schema left, Schema right, String prefixLeft, String prefixRight) {
        List<Schema.Attribute> result = new ArrayList<>(left.attributes.size() + right.attributes.size());
        result.addAll(prefixSchema(left.attributes, prefixLeft));
        result.addAll(prefixSchema(right.attributes, prefixRight));
        return new Schema(result);
    }

    public static void equalOrThrow(Schema left, Schema right) {
        if (!left.equals(right)) {
            throw new UnsupportedOperationException("schema mismatch");
        }
    }

    public static void containsOrThrow(Schema schema, String attributeName) {
        if (!schema.hasAttributeByName(attributeName)) {
            throw new UnsupportedOperationException("no such attribute with name '" +
                    attributeName + "'\navailable: " + schema.toString() + "\n");
        }
    }
}
