package ovgu.dbse.pinnecke.qdbs.operators;

import com.sun.org.apache.regexp.internal.RE;
import ovgu.dbse.pinnecke.qdbs.*;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class Difference extends BinaryOperator {

    private List<Record> list = new ArrayList<>();

    public Difference(Operator left, Operator right) {
        super("difference", left, right);
        Schemas.equalOrThrow(left.getSchema(), right.getSchema());
    }

    @Override
    public void open() {
        super.open();
        while (left.hasNext()) {
            list.add(left.next());
        }
        while (right.hasNext()) {
            list.remove(right.next());
        }
    }

    @Override
    public boolean hasNext() {
        return !list.isEmpty();
    }

    @Override
    public Record next() {
        Record next = list.iterator().next();
        list.remove(next);
        return next;
    }

    @Override
    public Schema getSchema() {
        return super.left.getSchema();
    }
}
