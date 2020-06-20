package com.adikastyle.kafka.connect.clickhouse;

import java.util.Collection;
import java.util.List;

public interface ClickhouseRepository {
    List<ColumnDef> describeTable(String tableName) throws Exception;
    int executeBatch(String tableName, List<ColumnDef> columns, Collection<String> values, Decoder decoder) throws Exception;
    void connect() throws Exception;
    void dispose() throws Exception;
}
