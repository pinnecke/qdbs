package ovgu.dbse.pinnecke.qdbs;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Records {

    public static Object[] combineFields(Record left, Record right) {
        Object[] l = left.getFields();
        Object[] r = right.getFields();
        List<Object> result = new ArrayList<>(l.length + r.length);
        result.addAll(Arrays.asList(l));
        result.addAll(Arrays.asList(r));
        return result.toArray();
    }

}
