package com.adikastyle.kafka.connect.clickhouse;

import java.util.Collection;

public interface ClickhouseService {
    int executeBatch(String tableName, Collection<String> batch) throws Exception;
    void refreshTablesMetadata() throws InterruptedException;
}
