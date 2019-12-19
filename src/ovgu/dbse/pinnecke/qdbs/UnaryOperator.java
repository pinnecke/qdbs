package ovgu.dbse.pinnecke.qdbs;

public abstract class UnaryOperator implements Operator {

    protected final Operator source;
    protected String outTableName;

    public UnaryOperator(String operatorName, Operator source) {
        this.outTableName = operatorName + "("+ source.getOutputTableName() + ")";
        this.source = source;
    }

    @Override
    public void open() {
        source.open();
    }

    @Override
    public void close() {
        source.close();
    }

    @Override
    public String getOutputTableName() {
        return outTableName;
    }
}
