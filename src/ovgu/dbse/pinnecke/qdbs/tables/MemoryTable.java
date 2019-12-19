package ovgu.dbse.pinnecke.qdbs.tables;

import ovgu.dbse.pinnecke.qdbs.Operator;
import ovgu.dbse.pinnecke.qdbs.Record;
import ovgu.dbse.pinnecke.qdbs.Schema;
import ovgu.dbse.pinnecke.qdbs.Table;

import java.util.ArrayList;
import java.util.List;

public class MemoryTable implements Table {

    private List<Schema.Attribute> attributes = new ArrayList<>();
    private List<Record> records = new ArrayList<>();

    public MemoryTable(Schema schema) {
        this(schema.getAttributes());
    }

    public MemoryTable(List<Schema.Attribute> attributes) {
        this.attributes.addAll(attributes);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{ \"schema\": [");
        sb.append(Schema.attributeListToString(this.attributes));
        sb.append("], \"records\": [");
        for (Operator it = this.fullScan(); it.hasNext(); ) {
            Record r = it.next();
            sb.append(r.toString());
            if (it.hasNext()) {
                sb.append(", ");
            }
        }
        sb.append("]}");
        return sb.toString();
    }

    @Override
    public Table insert(Object... values) {
        records.add(new Record(attributes, values));
        return this;
    }

    @Override
    public Table insertRecord(Record record) {
        return insert(record.getFields());
    }

    @Override
    public Schema getSchema() {
        return new Schema(attributes);
    }

    @Override
    public Operator fullScan() {
        return new MemoryTable.RecordReader(this);
    }

    public static class Builder {

        Schema.Builder schemaBuilder = new Schema.Builder();

        public MemoryTable.Builder addAttribute(String name, Schema.DataType type) {
            this.schemaBuilder = schemaBuilder.add(name, type);
            return this;
        }

        public MemoryTable create() {
            return new MemoryTable(schemaBuilder.getAttributes());
        }

    }

    private class RecordReader implements Operator {

        MemoryTable table;
        int idx;

        RecordReader(MemoryTable table) {
            this.table = table;
            this.idx = 0;
        }

        @Override
        public void open() {

        }

        @Override
        public boolean hasNext() {
            return idx < table.records.size();
        }

        @Override
        public Schema getSchema() {
            return table.getSchema();
        }

        @Override
        public Record next() {
            return table.records.get(idx++);
        }

        @Override
        public void close() {

        }

        @Override
        public String getOutputTableName() {
            return "temp-" + System.currentTimeMillis();
        }
    }
}
