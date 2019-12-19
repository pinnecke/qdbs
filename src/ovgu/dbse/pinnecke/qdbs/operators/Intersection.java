package ovgu.dbse.pinnecke.qdbs.operators;

import ovgu.dbse.pinnecke.qdbs.*;

import java.util.ArrayList;
import java.util.HashSet;

public class Intersection extends BinaryOperator {

    private HashSet<Record> testSet = new HashSet<>();
    private ArrayList<Record> outputList = new ArrayList<>();

    public Intersection(Operator left, Operator right) {
        super("intersect", left, right);
        Schemas.equalOrThrow(left.getSchema(), right.getSchema());
    }

    @Override
    public void open() {
        super.open();
        while (super.left.hasNext()) {
            testSet.add(super.left.next());
        }
        while (super.right.hasNext()) {
            Record record = super.right.next();
            if (testSet.contains(record))
                outputList.add(record);
        }
    }

    @Override
    public boolean hasNext() {
        return !outputList.isEmpty();
    }

    @Override
    public Record next() {
        Record next = outputList.iterator().next();
        outputList.remove(next);
        return next;
    }

    @Override
    public Schema getSchema() {
        return left.getSchema();
    }
}
