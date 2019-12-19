package ovgu.dbse.pinnecke.qdbs;

import ovgu.dbse.pinnecke.qdbs.tables.MemoryTable;

import java.io.PrintStream;

public class SilentEvaluator {

    private final Operator source;

    public SilentEvaluator(Operator source) {
        this.source = source;
    }

    public enum Silent {
        YES, NO
    }

    public void eval(Silent silent) {
        source.open();
        int i = 0;
        while (source.hasNext()) {
            i++;
            source.next();
        }
        if (silent.equals(Silent.NO)) {
            System.out.println(i + " record(s)");
        }
        source.close();
    }

}
