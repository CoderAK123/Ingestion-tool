package com.zeotap.dto;

import java.util.List;

public class ClickHouseTableRequest {
    private ClickHouseConfigDTO config;
    private String tableName;
    private String database;
 private List<String> columns;
    // Getters and Setters

    public ClickHouseConfigDTO getConfig() {
        return config;
    }

    public void setConfig(ClickHouseConfigDTO config) {
        this.config = config;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
    public String getDatabase() {
        return database;
    }

    public void setDatabase(String database) {
        this.database = database;
    }
    public List<String> getColumns() {
        return columns;
    }

    public void setColumns(List<String> columns) {
        this.columns = columns;
    }
}
