package com.zeotap.dto;

import java.util.List;

public class IngestionRequest {
    private ClickHouseConfigDTO config;
    private String tableName;
    private List<String> selectedColumns;
    private String outputFilePath;
    private String delimiter;
    private List<String> columns; 
    private ClickHouseTableRequest tableRequest;
    // Getters and Setters
// Getters and Setters
public ClickHouseTableRequest getTableRequest() {
    return tableRequest;
}

public void setTableRequest(ClickHouseTableRequest tableRequest) {
    this.tableRequest = tableRequest;
}
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

    public List<String> getSelectedColumns() {
        return selectedColumns;
    }

    public void setSelectedColumns(List<String> selectedColumns) {
        this.selectedColumns = selectedColumns;
    }

    public String getOutputFilePath() {
        return outputFilePath;
    }

    public void setOutputFilePath(String outputFilePath) {
        this.outputFilePath = outputFilePath;
    }


    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;}

        public String getDelimiter() {
            return delimiter;
        }
        private String joinQuery;

public String getJoinQuery() {
    return joinQuery;
}

public void setJoinQuery(String joinQuery) {
    this.joinQuery = joinQuery;
}

public List<String> getColumns() {
    return columns;
}

public void setColumns(List<String> columns) {
    this.columns = columns;
}

}
