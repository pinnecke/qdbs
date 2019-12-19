package ovgu.dbse.pinnecke;

import ovgu.dbse.pinnecke.qdbs.Operator;
import ovgu.dbse.pinnecke.qdbs.Record;
import ovgu.dbse.pinnecke.qdbs.Schema;
import ovgu.dbse.pinnecke.qdbs.UnaryOperator;


public class TupleCount extends UnaryOperator {

    private Schema operatorSchema;
    private int count;

    public TupleCount(Operator source) {
        super("tuple-count", source);
        operatorSchema = new Schema(new Schema.Attribute("count",
                Schema.DataType.INTEGER));
    }

    @Override
    public void open() {
        super.open();
        for (; source.hasNext(); source.next()) {
            count++;
        }
    }

    @Override
    public boolean hasNext() {
        return count > 0;
    }

    @Override
    public Schema getSchema() {
        return operatorSchema;
    }

    @Override
    public Record next() {
        Record result = new Record(operatorSchema, new Object[]{count});
        count = 0;
        return result;
    }
}