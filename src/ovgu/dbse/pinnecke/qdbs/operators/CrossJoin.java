package ovgu.dbse.pinnecke.qdbs.operators;

import ovgu.dbse.pinnecke.qdbs.*;
import ovgu.dbse.pinnecke.qdbs.tables.MemoryTable;

import java.util.ArrayDeque;
import java.util.Queue;

public class CrossJoin extends BinaryOperator {

    Schema schemaOut;
    MemoryTable leftTemp;
    int leftFieldIdx, rightFieldIdx;
    private Queue<Record> nextOuts = new ArrayDeque<>();

    public CrossJoin(Operator left, Operator right) {
        super("cross-join", left, right);
        schemaOut = Schemas.combine(left.getSchema(), right.getSchema(), left.getOutputTableName(), right.getOutputTableName());
    }

    @Override
    public void open() {
        super.open();
        left.open();
        leftTemp = new MemoryTable(left.getSchema().getAttributes());
        while (left.hasNext()) {
            Record in = left.next();
            leftTemp.insertRecord(in);
        }
        left.close();
    }

    @Override
    public boolean hasNext() {
        if (!nextOuts.isEmpty()) {
            return true;
        } else {
            if (super.right.hasNext()) {
                Record rhs = right.next();
                for (Operator scan = leftTemp.fullScan(); scan.hasNext(); ) {
                    Record lhs = scan.next();
                    nextOuts.add(new Record(schemaOut.getAttributes(), Records.combineFields(lhs, rhs)));
                }
                return !nextOuts.isEmpty();
            }
            return false;
        }
    }

    @Override
    public Schema getSchema() {
        return schemaOut;
    }

    @Override
    public Record next() {
        return nextOuts.poll();
    }
}