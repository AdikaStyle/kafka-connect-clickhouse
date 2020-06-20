package com.adikastyle.kafka.connect.clickhouse;

import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.sink.SinkTask;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ClickhouseSinkTask extends SinkTask {
    private Module m;
    private String tableName;

    public String version() {
        return null;
    }

    public void start(Map<String, String> map) {
        try {
            Module m = new Module(map);
            m.monitorSchemaChanges();
            tableName = m.getConfig().get(ClickhouseConfigDef.TABLE);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public void put(Collection<SinkRecord> collection) {
        List<String> strings = collection
                .stream()
                .map(item -> item.value().toString())
                .collect(Collectors.toList());


        try {
            this.m.getClickhouseService().executeBatch(tableName, strings);
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }

    public void stop() {
        try {
            m.dispose();
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
    }
}
