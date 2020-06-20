package com.adikastyle.kafka.connect.clickhouse;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

public class ClickhouseServiceImpl implements ClickhouseService {

    private List<ColumnDef> tableCache;
    private final ClickhouseRepository repository;
    private final Decoder decoder;
    private final int retryBackoff;
    private final int retryCount;

    private final ReentrantLock lock;

    public ClickhouseServiceImpl(ClickhouseRepository repository, Decoder decoder, int retryBackoff, int retryCount) throws Exception {
        this.tableCache = null;
        this.repository = repository;
        this.decoder = decoder;
        this.retryBackoff = retryBackoff;
        this.retryCount = retryCount;
        lock = new ReentrantLock();
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
        this.lock.lock();
        this.tableCache = null;
        this.lock.unlock();
    }

    private int writeBatch(String tableName, Collection<String> batch) throws Exception {
        this.lock.lock();
        if (this.tableCache == null)
            this.tableCache = this.repository.describeTable(tableName);
        this.lock.unlock();

        return this.repository.executeBatch(tableName, this.tableCache, batch, this.decoder);
    }
}
