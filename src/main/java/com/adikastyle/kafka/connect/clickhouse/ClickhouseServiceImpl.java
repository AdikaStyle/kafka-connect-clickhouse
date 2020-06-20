package com.adikastyle.kafka.connect.clickhouse;

import java.util.Collection;
import java.util.List;

public class ClickhouseServiceImpl implements ClickhouseService {

    private final LruCache<String, List<ColumnDef>> tablesCache;
    private final ClickhouseRepository repository;
    private final Decoder decoder;
    private final int retryBackoff;
    private final int retryCount;

    public ClickhouseServiceImpl(int cacheSize, ClickhouseRepository repository, Decoder decoder, int retryBackoff, int retryCount) throws Exception {
        this.tablesCache = new LruCache<>(cacheSize);
        this.repository = repository;
        this.decoder = decoder;
        this.retryBackoff = retryBackoff;
        this.retryCount = retryCount;
    }

    @Override
    public int executeBatch(String tableName, Collection<String> batch) throws Exception {
        int result = 0;

        for (int i = 0; i < this.retryCount; i++) {
            try {
               result = writeBatch(tableName, batch);
            } catch (Exception ex) {
                Thread.sleep(this.retryBackoff * (i+1));
            }
        }

        return result;
    }

    @Override
    public void refreshTablesMetadata() throws InterruptedException {
        for (String tableName : this.tablesCache.getKeysSnapshot()) {
            for (int i = 0; i < this.retryCount; i++) {
                try {
                    List<ColumnDef> columns = this.repository.describeTable(tableName);
                    this.tablesCache.put(tableName, columns);
                } catch (Exception ex) {
                    Thread.sleep(this.retryBackoff * (i+1));
                }
            }
        }
    }

    private int writeBatch(String tableName, Collection<String> batch) throws Exception {
        List<ColumnDef> tableDef = tablesCache.get(tableName);
        if (tableDef == null) {
            tableDef = this.repository.describeTable(tableName);
            tablesCache.put(tableName, tableDef);
        }

        return this.repository.executeBatch(tableName, tableDef, batch, this.decoder);
    }
}
