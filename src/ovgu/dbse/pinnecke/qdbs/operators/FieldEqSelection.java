package ovgu.dbse.pinnecke.qdbs.operators;

import ovgu.dbse.pinnecke.qdbs.*;
import ovgu.dbse.pinnecke.qdbs.tables.MemoryTable;

import java.util.function.Predicate;

public class FieldEqSelection extends UnaryOperator {

    private final Integer fieldIdxLeft, fieldIdxRight;
    private Table temp;
    private Operator out;

    public FieldEqSelection(Operator source, String attributeNameLeft, String attributeNameRight) {
        super("selection", source);
        Schemas.containsOrThrow(source.getSchema(), attributeNameLeft);
        Schemas.containsOrThrow(source.getSchema(), attributeNameRight);

        fieldIdxLeft = source.getSchema().getAttributeByName(attributeNameLeft).getLeft();
        fieldIdxRight = source.getSchema().getAttributeByName(attributeNameRight).getLeft();

        temp = new MemoryTable(source.getSchema());
    }

    @Override
    public void open() {
        super.open();

        while (super.source.hasNext()) {
            Record in = super.source.next();
            Object left = in.getAt(fieldIdxLeft);
            Object right = in.getAt(fieldIdxRight);
            if (left.equals(right)) {
                temp.insertRecord(in);
            }
        }

        out = temp.fullScan();
    }

    @Override
    public boolean hasNext() {
        return out.hasNext();
    }

    @Override
    public Schema getSchema() {
        return super.source.getSchema();
    }

    @Override
    public Record next() {
        return out.next();
    }
}