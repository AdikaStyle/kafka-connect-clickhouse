package com.adikastyle.kafka.connect.clickhouse;

import java.text.MessageFormat;
import java.util.List;
import java.util.StringJoiner;

public class InsertStatementHelper {

    public static String toInsertStatement(String tableName, List<ColumnDef> columns) {
        return MessageFormat.format(
                "INSERT INTO {1} VALUES ({2})",
                tableName,
                repeat("?", ",", columns.size())
        );
    }

    private static String repeat(String expression, String seperator, int count) {
        StringJoiner joiner = new StringJoiner(seperator);
        for (int i = 0; i < count; i++) {
            joiner.add(expression);
        }
        return joiner.toString();
    }

}
