package ovgu.dbse.pinnecke.qdbs;

import ovgu.dbse.pinnecke.qdbs.tables.MemoryTable;

import java.io.PrintStream;

public class PrettyPrinter {

    private final Operator source;
    private final Table table;

    public PrettyPrinter(Operator source) {
        table = new MemoryTable(source.getSchema().attributes);
        this.source = source;
    }

    public void print(PrintStream dst) {
        source.open();
        while (source.hasNext()) {
            table.insert(source.next().getFields());
        }
        source.close();
        Tables.printPretty(dst, table);
    }

}
