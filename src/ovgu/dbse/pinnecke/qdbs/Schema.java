package ovgu.dbse.pinnecke.qdbs;

import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class Schema implements Cloneable {

    List<Attribute> attributes = new ArrayList<>();

    public Schema(List<Attribute> attributes) {
        this.attributes.addAll(attributes);
    }

    public static String attributeListToString(List<Attribute> attributes) {
        return attributes.stream().map(Attribute::toString).collect(Collectors.joining(", "));
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Schema schema = (Schema) o;
        return Objects.equals(attributes, schema.attributes);
    }

    @Override
    public int hashCode() {

        return Objects.hash(attributes);
    }

    @Override
    public Object clone() {
        List<Attribute> cpy = new ArrayList<>(attributes.size());
        for (Attribute a : attributes) {
            cpy.add((Attribute) a.clone());
        }
        return new Schema(cpy);
    }

    public List<Attribute> getAttributes() {
        return attributes;
    }

    public boolean hasAttributeByName(String name) {
        return attributes.stream().anyMatch(attribute -> attribute.getName().equals(name));
    }

    public Pair<Integer, Attribute> getAttributeByName(String name) {
        MutablePair<Integer, Attribute> result = new MutablePair<>(null, null);
        for (int i = 0; i < attributes.size(); i++) {
            if (attributes.get(i).getName().equals(name)) {
                result.setLeft(i);
                result.setRight(attributes.get(i));
            }
        }
        return result;
    }

    @Override
    public String toString() {
        return Schema.attributeListToString(this.attributes);
    }

    public enum DataType {
        INTEGER, STRING
    }

    public static class Attribute implements Cloneable {

        private DataType type;
        private String name;

        Attribute(String name, DataType type) {
            this.name = name;
            this.type = type;
        }

        static Attribute readFromFile(RandomAccessFile file) throws IOException {
            return new Attribute(file.readUTF(), DataType.valueOf(file.readUTF()));
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Attribute attribute = (Attribute) o;
            return type == attribute.type &&
                    Objects.equals(name, attribute.name);
        }

        @Override
        public int hashCode() {

            return Objects.hash(type, name);
        }

        @Override
        protected Object clone() {
            return new Attribute(name, type);
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public DataType getType() {
            return type;
        }

        @Override
        public String toString() {
            return "{\"name\":\"" + name + "\", \"type\":\"" + type + "\"}";
        }

        void writeToFile(RandomAccessFile file) throws IOException {
            file.writeUTF(name);
            file.writeUTF(type.toString());
        }
    }

    public static class Builder {

        List<Attribute> attributes = new ArrayList<>();

        public Builder add(String name, DataType type) {
            this.attributes.add(new Attribute(name, type));
            return this;
        }

        Schema build() {
            return new Schema(attributes);
        }

        public List<Attribute> getAttributes() {
            return attributes;
        }

    }

}
