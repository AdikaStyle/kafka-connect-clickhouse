package com.adikastyle.kafka.connect.clickhouse;

import java.sql.*;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class ClickhouseRepositoryImpl implements ClickhouseRepository {
    private final Map<String, String> config;
    private Connection connection;
    private final LruCache<Integer, String> insertQueryCache; // Hash(columns) => Insert SQL Statement

    public ClickhouseRepositoryImpl(Map<String, String> config) {
        this.config = config;
        insertQueryCache = new LruCache<>(2048);
    }

    @Override
    public void connect() throws Exception {
        String uri = MessageFormat.format("jdbc:clickhouse://{1}", config.get(ClickhouseConfigDef.HOST));
        connection = DriverManager.getConnection(uri);
        if (connection.isValid(5000)) {
            throw new SQLException(MessageFormat.format("Cannot validate connection to {1}.", uri));
        }
    }

    @Override
    public List<ColumnDef> describeTable(String tableName) throws Exception {
        if (connection.isValid(5000)) {
            PreparedStatement stmt = connection.prepareStatement(MessageFormat.format("DESCRIBE {1}", tableName));
            ResultSet rs = stmt.executeQuery();

            List<ColumnDef> out = new ArrayList<>();
            int position = 1;
            while (rs.next()) {
                String name = rs.getString(1);
                String kind = rs.getString(2);
                out.add(new ColumnDef(position, name, kind));
                position++;
            }

            return out;
        }

        return new ArrayList<>();
    }

    @Override
    public int executeBatch(String tableName, List<ColumnDef> columns, Collection<String> values, Decoder decoder) throws Exception {
        String insertSql = this.insertQueryCache.get(columns.hashCode());
        if (insertSql == null) {
            insertSql = InsertStatementHelper.toInsertStatement(tableName, columns);
            this.insertQueryCache.put(columns.hashCode(), insertSql);
        }

        PreparedStatement stmt = this.connection.prepareStatement(insertSql);
        for (Decoder.Decodeable value : decoder.decode(values)) {
            for (ColumnDef column : columns) {
                stmt.setObject(column.getPosition(), value);
            }
            stmt.addBatch();
        }

        return sum(stmt.executeBatch());
    }

    @Override
    public void dispose() throws Exception {
        this.connection.close();
    }

    private int sum(int[] arr) {
        int sum = 0;
        for (int e : arr) {
            sum += e;
        }
        return sum;
    }
}
