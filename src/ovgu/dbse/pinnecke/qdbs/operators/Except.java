package ovgu.dbse.pinnecke.qdbs.operators;

import ovgu.dbse.pinnecke.qdbs.*;

import java.util.HashSet;
import java.util.Set;

public class Except extends BinaryOperator {

    Set<Record> testSet = new HashSet<>();
    private Record recordOut;

    public Except(Operator left, Operator right) {
        super(left, right);
        Schemas.equalOrThrow(left.getSchema(), right.getSchema());
    }

    @Override
    public void open() {
        super.open();
        right.open();
        while (right.hasNext()) {
            testSet.add(right.next());
        }
        right.close();
    }

    @Override
    public boolean hasNext() {
        if (super.left.hasNext()) {
            while (super.left.hasNext()) {
                Record in = super.left.next();
                if (!testSet.contains(in)) {
                    recordOut = in;
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public Schema getSchema() {
        return left.getSchema();
    }

    @Override
    public Record next() {
        return recordOut;
    }
}
