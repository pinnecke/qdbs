package ovgu.dbse.pinnecke.qdbs;

public interface Table {

    Table insert(Object... values);

    Table insertRecord(Record record);

    Schema getSchema();

    Operator fullScan();

}
