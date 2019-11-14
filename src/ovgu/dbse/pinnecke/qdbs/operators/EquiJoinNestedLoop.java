package ovgu.dbse.pinnecke.qdbs.operators;

import org.apache.commons.lang3.tuple.Pair;
import ovgu.dbse.pinnecke.qdbs.*;
import ovgu.dbse.pinnecke.qdbs.tables.MemoryTable;

import java.util.ArrayDeque;
import java.util.Queue;

public class EquiJoinNestedLoop extends BinaryOperator {

    Schema resultSchema;
    MemoryTable leftTemp;
    int leftFieldIdx, rightFieldIdx;
    private Queue<Record> nextOuts = new ArrayDeque<>();

    public EquiJoinNestedLoop(Operator left, Operator right, String attributeName) {
        super(left, right);
        if (!left.getSchema().hasAttributeByName(attributeName) ||
                !right.getSchema().hasAttributeByName(attributeName)) {
            throw new UnsupportedOperationException("no shared attribute with name '" + attributeName + "'");
        } else {
            Pair<Integer, Schema.Attribute> l = left.getSchema().getAttributeByName(attributeName);
            Pair<Integer, Schema.Attribute> r = right.getSchema().getAttributeByName(attributeName);
            if (l.getRight().getType() != r.getRight().getType()) {
                throw new UnsupportedOperationException("join attribute must have same data type");
            }
            leftFieldIdx = l.getLeft();
            rightFieldIdx = r.getLeft();
        }
        resultSchema = Schemas.combine(left.getSchema(), right.getSchema(), "L", "R");
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
                    Object lvalue = lhs.getAt(leftFieldIdx);
                    Object rvalue = rhs.getAt(rightFieldIdx);
                    if (lvalue.equals(rvalue)) {
                        nextOuts.add(new Record(resultSchema.getAttributes(), Records.combineFields(lhs, rhs)));
                    }
                }
                return !nextOuts.isEmpty();
            }
            return false;
        }
    }

    @Override
    public Schema getSchema() {
        return resultSchema;
    }

    @Override
    public Record next() {
        return nextOuts.poll();
    }
}
