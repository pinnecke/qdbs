package ovgu.dbse.pinnecke.qdbs.operators;

import org.apache.commons.lang3.tuple.Pair;
import ovgu.dbse.pinnecke.qdbs.*;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Projection extends UnaryOperator {

    List<Schema.Attribute> dstAttributes = new ArrayList<>();
    List<Integer> srcAttributeidx = new ArrayList<>();

    public Projection(Operator source, String... attributeNames) {
        super("projection", source);

        List<Pair<Integer, Schema.Attribute>> tmpList = new ArrayList<>();

        for (String name : attributeNames) {
            Schemas.containsOrThrow(source.getSchema(), name);
            Pair<Integer, Schema.Attribute> attr = source.getSchema().getAttributeByName(name);
            tmpList.add(attr);
        }

        dstAttributes.addAll(tmpList.stream().map(Pair::getRight).collect(Collectors.toList()));
        srcAttributeidx.addAll(tmpList.stream().map(Pair::getLeft).collect(Collectors.toList()));
    }

    @Override
    public boolean hasNext() {
        return source.hasNext();
    }

    @Override
    public Schema getSchema() {
        return new Schema(dstAttributes);
    }

    @Override
    public Record next() {
        Record in = source.next();
        return new Record(dstAttributes, in.getSomeFields(srcAttributeidx));
    }
}
