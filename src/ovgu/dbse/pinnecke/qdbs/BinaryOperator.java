package ovgu.dbse.pinnecke.qdbs;

public abstract class BinaryOperator implements Operator {

    protected final Operator left;
    protected final Operator right;

    public BinaryOperator(Operator left, Operator right) {
        this.left = left;
        this.right = right;
    }

    @Override
    public void open() {
        left.open();
        right.open();
    }

    @Override
    public void close() {
        left.close();
        right.close();
    }
}
