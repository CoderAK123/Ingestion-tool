package com.zeotap.service;

import com.zeotap.dto.ClickHouseConfigDTO;
import org.springframework.stereotype.Service;

import java.io.BufferedReader;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.*;

@Service
public class FlatFileIngestionService {

    private Connection createConnection(ClickHouseConfigDTO config) throws Exception {
        String url = String.format("jdbc:clickhouse://%s:%d/%s", config.getHost(), config.getPort(), config.getDatabase());
        Properties props = new Properties();
        props.setProperty("user", config.getUsername());
        props.setProperty("ssl", config.isUseHttps() ? "true" : "false");
        props.setProperty("access_token", config.getPassword());
        return DriverManager.getConnection(url, props);
    }

    public int ingestCSVToClickHouse(ClickHouseConfigDTO config, String tableName, String filePath, String delimiter) {
        int count = 0;

        try (BufferedReader br = new BufferedReader(new FileReader(filePath));
             Connection conn = createConnection(config);
             Statement stmt = conn.createStatement()) {

            String headerLine = br.readLine();
            if (headerLine == null) return 0;

            String[] columns = headerLine.split(delimiter);
            StringBuilder createSQL = new StringBuilder("CREATE TABLE IF NOT EXISTS " + tableName + " (");

            for (int i = 0; i < columns.length; i++) {
                createSQL.append(columns[i]).append(" String");
                if (i < columns.length - 1) createSQL.append(", ");
            }
            createSQL.append(") ENGINE = MergeTree() ORDER BY tuple();");
            stmt.execute(createSQL.toString());

            String line;
            while ((line = br.readLine()) != null) {
                String[] values = line.split(delimiter);
                StringBuilder insertSQL = new StringBuilder("INSERT INTO " + tableName + " VALUES (");

                for (int i = 0; i < values.length; i++) {
                    insertSQL.append("'").append(values[i].replace("'", "''")).append("'");
                    if (i < values.length - 1) insertSQL.append(", ");
                }
                insertSQL.append(");");

                stmt.execute(insertSQL.toString());
                count++;
            }

        } catch (Exception e) {
            e.printStackTrace();
        }

        return count;
    }

private String joinQuery;

public String getJoinQuery() {
    return joinQuery;
}

public void setJoinQuery(String joinQuery) {
    this.joinQuery = joinQuery;
}

}
