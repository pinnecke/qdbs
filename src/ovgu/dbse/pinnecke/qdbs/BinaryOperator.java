package ovgu.dbse.pinnecke.qdbs;

public abstract class BinaryOperator implements Operator {

    protected final Operator left;
    protected final Operator right;
    protected final String outTableName;

    public BinaryOperator(String operatorName, Operator left, Operator right) {
        this.outTableName = operatorName + "("+ left.getOutputTableName() + ", " + right.getOutputTableName() + ")";
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

    @Override
    public String getOutputTableName() {
        return outTableName;
    }
}
