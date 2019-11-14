package ovgu.dbse.pinnecke;

import ovgu.dbse.pinnecke.dbs1.Database;
import ovgu.dbse.pinnecke.qdbs.Operator;
import ovgu.dbse.pinnecke.qdbs.PrettyPrinter;
import ovgu.dbse.pinnecke.qdbs.Table;
import ovgu.dbse.pinnecke.qdbs.operators.*;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Main {

    public static void main(String[] args) throws Exception {

        Table kundenTable = Database.loadKundenTable();
        Table haendlerTable = Database.loadHaendlerTable();
        Table artikelTable = Database.loadArtikelTable();
        Table bietetanTable = Database.loadBietetAnTable();
        Table bestellungTable = Database.loadBestellungTable();
        Table istaufTable = Database.loadIstAufTable();

        // ---------------------------------------------------------------------------
        //  Task 4.1.1
        // ---------------------------------------------------------------------------
        {
            Operator select = new Projection(istaufTable.fullScan(), "Bnr");
            new PrettyPrinter(select).print(System.out);
        }

        // ---------------------------------------------------------------------------
        //  Task 4.1.2
        // ---------------------------------------------------------------------------
        {
            Operator join = new EquiJoinNestedLoop(haendlerTable.fullScan(),
                    bestellungTable.fullScan(), "Hnr");
            Operator select = new Projection(join, "L.Name");
            Operator rename = new Rename(select, "L.Name", "Name");
            Operator dedup = new Dedup(rename);
            new PrettyPrinter(dedup).print(System.out);
        }

        // ---------------------------------------------------------------------------
        //  Task 4.1.3
        // ---------------------------------------------------------------------------
        {
            Operator left = new Projection(haendlerTable.fullScan(), "Hnr");
            Operator right = new Projection(bietetanTable.fullScan(), "Hnr");
            Operator except = new Except(left, right);
            new PrettyPrinter(except).print(System.out);
        }

        // ---------------------------------------------------------------------------
        //  Task 4.1.4
        // ---------------------------------------------------------------------------
        {
            Operator left = new Filter(bestellungTable.fullScan(), "Datum", field ->
                    compareDate(field, "01.03.2003", true)
            );
            Operator right = new Filter(bestellungTable.fullScan(), "Datum", field ->
                    compareDate(field, "01.05.2003", false));
            Operator union = new Union(left, right);
            new PrettyPrinter(union).print(System.out);
        }

        // ---------------------------------------------------------------------------
        //  Task 4.1.5
        // ---------------------------------------------------------------------------
        {
            Operator join1 = new EquiJoinNestedLoop(kundenTable.fullScan(),
                    bestellungTable.fullScan(), "Knr");
            Operator rename1 = new Rename(join1, "R.Bnr", "Bnr");
            Operator join2 = new EquiJoinNestedLoop(rename1,
                    istaufTable.fullScan(), "Bnr");
            Operator rename2 = new Rename(join2, "R.Anr", "Anr");
            Operator join3 = new EquiJoinNestedLoop(rename2,
                    artikelTable.fullScan(), "Anr");
            Operator project = new Projection(join3,
                    "L.L.L.Knr", "L.L.L.Name", "L.L.Bnr", "L.L.R.Hnr", "L.L.R.Datum",
                    "L.Anr", "L.R.Anzahl", "R.Bez.");
            Operator rename3 = new Rename(project, "L.L.L.Knr", "Knr");
            Operator rename4 = new Rename(rename3, "L.L.L.Name", "Name");
            Operator rename5 = new Rename(rename4, "L.L.Bnr", "Bnr");
            Operator rename6 = new Rename(rename5, "L.L.R.Hnr", "Hnr");
            Operator rename7 = new Rename(rename6, "L.L.R.Datum", "Datum");
            Operator rename8 = new Rename(rename7, "L.Anr", "Anr");
            Operator rename9 = new Rename(rename8, "L.R.Anzahl", "Anzahl");
            Operator rename10 = new Rename(rename9, "R.Bez.", "Bezeichnung");

            new PrettyPrinter(rename10).print(System.out);
        }

    }

    static boolean compareDate(Object field, String compareWith, boolean before) {
        try {
            Date a = new SimpleDateFormat("dd.MM.yyyy").parse((String) field);
            Date b = new SimpleDateFormat("dd.MM.yyyy").parse(compareWith);
            return before ? a.before(b) : a.after(b);
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

}
