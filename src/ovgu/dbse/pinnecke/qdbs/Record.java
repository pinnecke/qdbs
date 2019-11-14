package ovgu.dbse.pinnecke.qdbs;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class Record {

    private List<Schema.Attribute> schema;
    private List<Object> fields = new ArrayList<>();

    public Record(List<Schema.Attribute> schema, Object[] values) {
        this.schema = schema;
        if (values.length != schema.size()) {
            throw new RuntimeException("Illegal number of fields");
        }
        for (int i = 0; i < schema.size(); i++) {
            Schema.DataType type = schema.get(i).getType();
            if (type == Schema.DataType.STRING && values[i] instanceof String) {
                fields.add(values[i]);
            } else if (type == Schema.DataType.INTEGER && values[i] instanceof Integer) {
                fields.add(values[i]);
            } else {
                throw new RuntimeException("Illegal type mapping");
            }
        }
    }

    public static Record readFromFile(RandomAccessFile file, List<Schema.Attribute> schema) throws IOException {

        List<Object> fields = new ArrayList<>(schema.size());

        for (Schema.Attribute attribute : schema) {
            switch (attribute.getType()) {
                case STRING:
                    fields.add(file.readUTF());
                    break;
                case INTEGER:
                    fields.add(file.readInt());
                    break;
                default:
                    throw new RuntimeException("Illegal type detected");
            }
        }

        return new Record(schema, fields.toArray(new Object[0]));
    }

    public Object[] getFields() {
        return fields.toArray(new Object[0]);
    }

    public Object[] getSomeFields(List<Integer> idxs) {
        List<Object> result = new ArrayList<>(idxs.size());
        for (Integer idx : idxs) {
            result.add(fields.get(idx));
        }
        return result.toArray();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Record record = (Record) o;
        return Objects.equals(fields, record.fields);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fields);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < fields.size(); i++) {
            Object o = fields.get(i);
            sb.append(o instanceof String ? enquote(o) : o);
            sb.append(i + 1 < fields.size() ? ", " : "");
        }
        sb.append("]");
        return sb.toString();
    }

    private String enquote(Object o) {
        return "\"" + o + "\"";
    }

    public void writeToFile(RandomAccessFile file) throws IOException {
        for (Object o : fields) {
            if (o instanceof String) {
                file.writeUTF((String) o);
            } else if (o instanceof Integer) {
                file.writeInt((Integer) o);
            } else {
                throw new RuntimeException("Unknown type detected");
            }
        }
    }

    public String getStringAt(int i) {
        return (String) fields.get(i);
    }

    public Object getAt(int i) {
        return fields.get(i);
    }

    public Integer getIntegerAt(int i) {
        return (Integer) fields.get(i);
    }
}
