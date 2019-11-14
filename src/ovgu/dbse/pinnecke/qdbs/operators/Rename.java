package ovgu.dbse.pinnecke.qdbs.operators;

import ovgu.dbse.pinnecke.qdbs.Operator;
import ovgu.dbse.pinnecke.qdbs.Record;
import ovgu.dbse.pinnecke.qdbs.Schema;
import ovgu.dbse.pinnecke.qdbs.UnaryOperator;

import java.util.NoSuchElementException;

public class Rename extends UnaryOperator {

    Schema dstSchema;

    public Rename(Operator source, String attributeOld, String attributeNew) {
        super(source);
        if (!source.getSchema().hasAttributeByName(attributeOld)) {
            throw new NoSuchElementException("no such attribute");
        }
        dstSchema = (Schema) source.getSchema().clone();
        dstSchema.getAttributeByName(attributeOld).getRight().setName(attributeNew);
    }

    @Override
    public boolean hasNext() {
        return source.hasNext();
    }

    @Override
    public Schema getSchema() {
        return dstSchema;
    }

    @Override
    public Record next() {
        return source.next();
    }
}
