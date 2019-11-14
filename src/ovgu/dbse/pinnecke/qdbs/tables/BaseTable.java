package ovgu.dbse.pinnecke.qdbs.tables;

import ovgu.dbse.pinnecke.qdbs.*;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.List;

public class BaseTable implements Table {

    Mode mode;
    private String name;
    private RandomAccessFile file;
    private long instanceStartOffset;
    private List<Schema.Attribute> attributes;
    private String filePath;

    static BaseTable createEmpty(String filePath) throws IOException {
        BaseTable result = readDatabaseHeader(filePath);
        result.mode = Mode.READ_WRITE;
        return result;
    }

    public static BaseTable open(String filePath) throws IOException {
        BaseTable result = readDatabaseHeader(filePath);
        result.mode = Mode.READ_ONLY;
        return result;
    }

    private static BaseTable readDatabaseHeader(String filePath) throws IOException {
        BaseTable result = new BaseTable();
        result.file = new RandomAccessFile(filePath, "rw");
        result.filePath = filePath;
        result.name = result.file.readUTF();
        result.attributes = Schemas.attributeListFromFile(result.file);
        result.instanceStartOffset = result.file.getFilePointer();
        return result;
    }

    @Override
    public Table insert(Object... values) {
        try {
            file.seek(file.length());
            new Record(attributes, values).writeToFile(file);
        } catch (IOException e) {
            e.printStackTrace();
        }
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
        try {
            return new RecordReader(this);
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{ \"name\": \"");
        sb.append(this.name);
        sb.append("\", \"schema\": [");
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

    private String getFilePath() {
        return filePath;
    }

    private void rewind() throws IOException {
        file.seek(instanceStartOffset);
    }

    enum Mode {READ_WRITE, READ_ONLY}

    public static class Builder {

        Schema.Builder schemaBuilder = new Schema.Builder();
        String tableName;

        public Builder(String tableName) {
            this.tableName = tableName;
        }

        public Builder addAttribute(String name, Schema.DataType type) {
            this.schemaBuilder = schemaBuilder.add(name, type);
            return this;
        }

        public BaseTable create(String filePath) throws IOException {
            RandomAccessFile file = new RandomAccessFile(filePath, "rw");
            file.writeUTF(tableName);
            Schemas.attributeListToFile(file, schemaBuilder.getAttributes());
            file.close();
            return createEmpty(filePath);
        }

    }

    private class RecordReader implements Operator {

        BaseTable table;

        RecordReader(BaseTable table) throws IOException {
            this.table = BaseTable.open(table.getFilePath());
        }

        @Override
        public void open() {
            try {
                this.table.rewind();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        @Override
        public boolean hasNext() {
            try {
                return this.table.file.getFilePointer() < this.table.file.length();
            } catch (IOException e) {
                e.printStackTrace();
                return false;
            }
        }

        @Override
        public Schema getSchema() {
            return table.getSchema();
        }

        @Override
        public Record next() {
            try {
                return Record.readFromFile(this.table.file, this.table.attributes);
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }

        @Override
        public void close() {

        }
    }

}
