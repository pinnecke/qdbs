package ovgu.dbse.pinnecke.qdbs.operators;

import ovgu.dbse.pinnecke.qdbs.*;

public class Union extends BinaryOperator {

    private boolean leftHasNext;

    public Union(Operator left, Operator right) {
        super(left, right);
        Schemas.equalOrThrow(left.getSchema(), right.getSchema());
    }

    @Override
    public boolean hasNext() {
        leftHasNext = left.hasNext();
        if (leftHasNext) {
            return true;
        } else {
            return right.hasNext();
        }
    }

    @Override
    public Schema getSchema() {
        return left.getSchema();
    }

    @Override
    public Record next() {
        if (leftHasNext) {
            return super.left.next();
        } else {
            return super.right.next();
        }
    }
}
