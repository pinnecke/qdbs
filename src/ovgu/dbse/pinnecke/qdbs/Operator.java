package ovgu.dbse.pinnecke.qdbs;

public interface Operator {

    void open();

    boolean hasNext();

    Schema getSchema();

    Record next();

    void close();

    String getOutputTableName();
}
