package ovgu.dbse.pinnecke.qdbs.operators;

import ovgu.dbse.pinnecke.qdbs.*;

import java.util.HashSet;
import java.util.Iterator;
import java.util.TreeSet;

public class Dedup extends UnaryOperator {

    private HashSet<Record> set = new HashSet<>();

    public Dedup(Operator source) {
        super("distinct", source);
    }

    @Override
    public void open() {
        super.open();
        while (source.hasNext()) {
            Record record = source.next();
            set.add(record);
        }
    }

    @Override
    public boolean hasNext() {
        return !set.isEmpty();
    }

    @Override
    public Record next() {
        Record next = set.iterator().next();
        set.remove(next);
        return next;
    }

    @Override
    public Schema getSchema() {
        return super.source.getSchema();
    }
}
