package com.adikastyle.kafka.connect.clickhouse;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.sink.SinkConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ClickhouseSink extends SinkConnector {
    private String host;
    private String username;
    private String password;
    private String database;
    private String table;
    private String tableSelector;

    public void start(Map<String, String> map) {
        host = map.get(ClickhouseConfigDef.HOST);
        username = map.get(ClickhouseConfigDef.USERNAME);
        password = map.get(ClickhouseConfigDef.PASSWORD);
        database = map.get(ClickhouseConfigDef.DATABASE);
        table = map.get(ClickhouseConfigDef.TABLE);
        tableSelector = map.get(ClickhouseConfigDef.TABLE_SELECTOR);
    }

    public void stop() {

    }

    public List<Map<String, String>> taskConfigs(int maxTasks) {
        List<Map<String, String>> configs = new ArrayList<Map<String, String>>();

        for (int i = 0; i < maxTasks; i++) {
            Map<String, String> config = new HashMap<String, String>();
            config.put(ClickhouseConfigDef.HOST, host);
            config.put(ClickhouseConfigDef.USERNAME, username);
            config.put(ClickhouseConfigDef.PASSWORD, password);
            config.put(ClickhouseConfigDef.DATABASE, database);
            config.put(ClickhouseConfigDef.TABLE, table);
            config.put(ClickhouseConfigDef.TABLE_SELECTOR, tableSelector);

            configs.add(config);
        }

        return configs;
    }

    public Class<? extends Task> taskClass() {
        return ClickhouseSinkTask.class;
    }

    public ConfigDef config() {
        return ClickhouseConfigDef.build();
    }

    public String version() {
        return ClickhouseConfigDef.version();
    }
}
