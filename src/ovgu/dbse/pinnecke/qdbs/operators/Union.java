package ovgu.dbse.pinnecke.qdbs.operators;

import ovgu.dbse.pinnecke.qdbs.*;

public class Union extends BinaryOperator {

    private boolean leftHasNext;

    public Union(Operator left, Operator right) {
        super("union", left, right);
        Schemas.equalOrThrow(left.getSchema(), right.getSchema());
    }

    @Override
    public boolean hasNext() {
        return ((leftHasNext = left.hasNext())) || right.hasNext();
    }

    @Override
    public Schema getSchema() {
        return left.getSchema();
    }

    @Override
    public Record next() {
        return leftHasNext ? super.left.next() : super.right.next();
    }
}
