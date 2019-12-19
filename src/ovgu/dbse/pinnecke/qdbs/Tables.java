package ovgu.dbse.pinnecke.qdbs;

import org.apache.commons.lang3.StringUtils;

import java.io.PrintStream;

public class Tables {

    public static void printPretty(PrintStream dst, Table table) {
        Schema schema = table.getSchema();
        int max = schema.attributes.size();
        for (int i = 0; i < max; i++) {
            Schema.Attribute a = schema.attributes.get(i);
            dst.printf("%s%s", StringUtils.center(a.getName(), a.getType() == Schema.DataType.INTEGER ? 72 : 80), i + 1 < max ? "|" : " ");
        }
        dst.println();
        for (int i = 0; i < max; i++) {
            Schema.Attribute a = schema.attributes.get(i);
            for (int j = 0; j < (a.getType() == Schema.DataType.INTEGER ? 72 : 80); j++) {
                dst.print("-");
            }
            dst.printf("%s", i + 1 < max ? "+" : "-");
        }
        dst.println();

        int numRecords = 0;
        for (Operator it = table.fullScan(); it.hasNext(); numRecords++) {
            Record r = it.next();
            for (int i = 0; i < max; i++) {
                Schema.Attribute a = schema.attributes.get(i);
                if (a.getType() == Schema.DataType.STRING) {
                    dst.printf(" %1$-78s ", r.getStringAt(i));
                    dst.printf("%s", i + 1 < max ? "|" : "");
                } else {
                    dst.printf("%1$71s ", r.getIntegerAt(i).toString());
                    dst.printf("%s", i + 1 < max ? "|" : "");
                }
            }
            dst.println();
        }
        dst.println("\n" + numRecords + " records\n\n");
    }

}
