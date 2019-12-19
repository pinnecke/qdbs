package ovgu.dbse.pinnecke;

import org.apache.commons.math3.stat.descriptive.moment.Mean;
import ovgu.dbse.pinnecke.dbs1.Database;
import ovgu.dbse.pinnecke.qdbs.*;
import ovgu.dbse.pinnecke.qdbs.SilentEvaluator.*;
import ovgu.dbse.pinnecke.qdbs.operators.*;
import ovgu.dbse.pinnecke.qdbs.tables.BaseTable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.BiConsumer;

public class Main {

    public static void main(String[] args) throws Exception {

        System.out.println("** Benchmarking Query Execution over 10.000 Runs **");

        double q1Runtime = run((haendlerTable, bestellungTable) -> {
            Operator haendler = haendlerTable.fullScan();
            Operator filterHaendler = new Selection(haendler, "Name", (obj) -> ((String) obj).equals("G.Hals"));
            Operator bestellungen = bestellungTable.fullScan();
            Operator cross = new CrossJoin(filterHaendler, bestellungen);
            Operator on = new FieldEqSelection(cross,  "selection(Haendler).Hnr", "Bestellung.Hnr");
            Operator select = new Projection(on, "Bestellung.Datum");
            Operator distinct = new Dedup(select);
            new SilentEvaluator(distinct).eval(Silent.YES);
        });

        double q2Runtime = run((haendlerTable, bestellungTable) -> {
            Operator haendler = haendlerTable.fullScan();
            Operator bestellungen = bestellungTable.fullScan();
            Operator cross = new CrossJoin(haendler, bestellungen);
            Operator on = new FieldEqSelection(cross,  "Haendler.Hnr", "Bestellung.Hnr");
            Operator filterHaendler = new Selection(on, "Haendler.Name", (obj) -> ((String) obj).equals("G.Hals"));
            Operator select = new Projection(filterHaendler, "Bestellung.Datum");
            Operator distinct = new Dedup(select);
            new SilentEvaluator(distinct).eval(Silent.YES);
        });

        System.out.println("Q1 mean wallclock runtime: " + q1Runtime + " ms");
        System.out.println("Q2 mean wallclock runtime: " + q2Runtime + " ms");
    }

    private static double run(BiConsumer<BaseTable, BaseTable> query) throws IOException {
            List<Double> runtimes = new ArrayList<>();

            for (int i = 0; i < 20000; i++) {
                BaseTable haendlerTable = Database.loadHaendlerTable();
                BaseTable bestellungTable = Database.loadBestellungTable();

                long t1 = System.currentTimeMillis();
                query.accept(haendlerTable, bestellungTable);
                long t2 = System.currentTimeMillis();

                if (i > 10000) {
                    /* skip the first runs to avoid JIT compilation influence */
                    runtimes.add((double) (t2 - t1));
                }

                haendlerTable.close();
                bestellungTable.close();
            }

        return new Mean().evaluate(runtimes.stream().mapToDouble(d -> d).toArray());
    }
}
