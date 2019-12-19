package ovgu.dbse.pinnecke.qdbs.operators;

import ovgu.dbse.pinnecke.qdbs.*;

import java.util.function.Predicate;

public class Selection extends UnaryOperator {

    private final Integer fieldIdx;
    Predicate<Object> pred;
    private Record nextOut;

    public Selection(Operator source, String attributeName, Predicate<Object> pred) {
        super("selection", source);
        Schemas.containsOrThrow(source.getSchema(), attributeName);
        fieldIdx = source.getSchema().getAttributeByName(attributeName).getLeft();
        this.pred = pred;
    }

    @Override
    public boolean hasNext() {
        while (super.source.hasNext()) {
            Record in = super.source.next();
            Object field = in.getAt(fieldIdx);
            if (pred.test(field)) {
                nextOut = in;
                return true;
            }
        }
        return false;
    }

    @Override
    public Schema getSchema() {
        return super.source.getSchema();
    }

    @Override
    public Record next() {
        return nextOut;
    }
}