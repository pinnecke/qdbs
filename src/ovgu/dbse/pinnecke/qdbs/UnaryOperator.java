package ovgu.dbse.pinnecke.qdbs;

public abstract class UnaryOperator implements Operator {

    protected final Operator source;

    public UnaryOperator(Operator source) {
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
}
