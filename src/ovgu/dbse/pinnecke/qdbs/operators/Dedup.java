package ovgu.dbse.pinnecke.qdbs.operators;

import ovgu.dbse.pinnecke.qdbs.Operator;
import ovgu.dbse.pinnecke.qdbs.Record;
import ovgu.dbse.pinnecke.qdbs.Schema;
import ovgu.dbse.pinnecke.qdbs.UnaryOperator;

import java.util.HashSet;
import java.util.Set;

public class Dedup extends UnaryOperator {

    Set<Record> testSet = new HashSet<>();
    private Record nextOut;

    public Dedup(Operator source) {
        super(source);
    }

    @Override
    public boolean hasNext() {
        while (source.hasNext()) {
            Record in = source.next();
            if (!testSet.contains(in)) {
                testSet.add(in);
                nextOut = in;
                return true;
            }
        }
        return false;
    }

    @Override
    public Schema getSchema() {
        return super.source.getSchema();
    }

    @Override
    public Record next() {
        return nextOut;
    }
}
