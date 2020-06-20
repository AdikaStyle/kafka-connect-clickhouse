package com.adikastyle.kafka.connect.clickhouse;

import org.apache.kafka.common.config.ConfigDef;

public class ClickhouseConfigDef {
    public static final String HOST = "host";
    public static final String DATABASE = "db";
    public static final String TABLE = "table";
    public static final String USERNAME = "username";
    public static final String PASSWORD = "password";

    public static ConfigDef build() {
        ConfigDef cd = new ConfigDef();

        cd.define(
                HOST,
                ConfigDef.Type.STRING,
                "jdbc:clickhouse://localhost:9000",
                ConfigDef.Importance.HIGH,
                "clickhouse host uri"
        );

        cd.define(
                DATABASE,
                ConfigDef.Type.STRING,
                "default",
                ConfigDef.Importance.HIGH,
                "The database you wish to connect to"
        );

        cd.define(
                TABLE,
                ConfigDef.Type.STRING,
                "",
                ConfigDef.Importance.HIGH,
                "The table you wish to write to"
        );

        cd.define(
                USERNAME,
                ConfigDef.Type.STRING,
                "",
                ConfigDef.Importance.HIGH,
                "The username used to connect to clickhouse"
        );

        cd.define(
                PASSWORD,
                ConfigDef.Type.STRING,
                "",
                ConfigDef.Importance.HIGH,
                "The password used to connect to clickhouse"
        );

        return cd;
    }

    public static String version() {
        return "0.1";
    }
}
