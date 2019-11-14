package ovgu.dbse.pinnecke.dbs1;

import ovgu.dbse.pinnecke.qdbs.Schema;
import ovgu.dbse.pinnecke.qdbs.Table;
import ovgu.dbse.pinnecke.qdbs.tables.BaseTable;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.function.Consumer;

public class Database {


    public static Table loadKundenTable() throws IOException {
        return createTable("kunde.db", "Kunde",
                (builder) ->
                        builder
                                .addAttribute("Knr", Schema.DataType.INTEGER)
                                .addAttribute("Name", Schema.DataType.STRING)
                ,
                (newTable) ->
                        newTable
                                .insert(13, "M. Müller")
                                .insert(17, "A. Meier")
                                .insert(23, "I. Schulze")
        );
    }

    public static Table loadHaendlerTable() throws IOException {
        return createTable("haendler.db", "Haendler",
                (builder) ->
                        builder
                                .addAttribute("Hnr", Schema.DataType.INTEGER)
                                .addAttribute("Name", Schema.DataType.STRING)
                ,
                (newTable) ->
                        newTable
                                .insert(5, "G.Hals")
                                .insert(7, "P.Schmidt")
                                .insert(11, "E.Meier")
                                .insert(13, "E.Mueller")
        );
    }

    public static Table loadArtikelTable() throws IOException {
        return createTable("artikel.db", "Artikel",
                (builder) ->
                        builder
                                .addAttribute("Anr", Schema.DataType.INTEGER)
                                .addAttribute("Bez.", Schema.DataType.STRING)
                ,
                (newTable) ->
                        newTable
                                .insert(45, "Steckernetzteil")
                                .insert(57, "TP-Kabel")
                                .insert(67, "Einbaukäfig")
        );
    }

    public static Table loadBietetAnTable() throws IOException {
        return createTable("bietet_an.db", "bietet_an",
                (builder) ->
                        builder
                                .addAttribute("Hnr", Schema.DataType.INTEGER)
                                .addAttribute("Anr", Schema.DataType.INTEGER)
                ,
                (newTable) ->
                        newTable
                                .insert(5, 45)
                                .insert(5, 57)
                                .insert(7, 67)
                                .insert(7, 45)
                                .insert(11, 57)
                                .insert(5, 67)
                                .insert(11, 57)
        );
    }

    public static Table loadBestellungTable() throws IOException {
        return createTable("bestellung.db", "Bestellung",
                (builder) ->
                        builder
                                .addAttribute("Bnr", Schema.DataType.INTEGER)
                                .addAttribute("Hnr", Schema.DataType.INTEGER)
                                .addAttribute("Datum", Schema.DataType.STRING)
                                .addAttribute("Knr", Schema.DataType.INTEGER)
                ,
                (newTable) ->
                        newTable
                                .insert(3, 7, "01.12.2002", 17)
                                .insert(5, 11, "27.04.2003", 23)
                                .insert(7, 5, "13.05.2003", 17)
                                .insert(10, 5, "01.09.2003", 13)

        );
    }

    public static Table loadIstAufTable() throws IOException {
        return createTable("ist_auf.db", "ist_auf",
                (builder) ->
                        builder
                                .addAttribute("Bnr", Schema.DataType.INTEGER)
                                .addAttribute("Anr", Schema.DataType.INTEGER)
                                .addAttribute("Anzahl", Schema.DataType.INTEGER)
                ,
                (newTable) ->
                        newTable
                                .insert(3, 45, 1)
                                .insert(3, 67, 5)
                                .insert(5, 67, 1)
                                .insert(7, 57, 3)
                                .insert(7, 67, 2)
                                .insert(10, 45, 2)
                                .insert(10, 57, 5)
                                .insert(10, 67, 3)
        );
    }


    private static BaseTable createTable(String fileName, String tableName, Consumer<BaseTable.Builder> defSchema,
                                         Consumer<BaseTable> insertData) throws IOException {

        BaseTable table;

        if (Files.exists(Paths.get(fileName))) {
            table = BaseTable.open(fileName);
        } else {
            BaseTable.Builder builder = new BaseTable.Builder(tableName);
            defSchema.accept(builder);
            table = builder.create(fileName);
            insertData.accept(table);
        }

        return table;
    }

}
