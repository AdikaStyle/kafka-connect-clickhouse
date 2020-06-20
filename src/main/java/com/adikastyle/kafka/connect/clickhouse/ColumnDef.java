package com.adikastyle.kafka.connect.clickhouse;

public class ColumnDef {
    private final int position;
    private final String name, kind;

    public ColumnDef(int position, String name, String kind) {
        this.position = position;
        this.name = name;
        this.kind = kind;
    }

    public int getPosition() {
        return position;
    }

    public String getName() {
        return name;
    }

    public String getKind() {
        return kind;
    }
}
