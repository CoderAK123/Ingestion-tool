package com.zeotap.config;

import com.clickhouse.jdbc.ClickHouseDataSource;
import java.sql.SQLException;

public class ClickHouseJDBCConfig {

    // Adjust this URL based on your Docker setup (localhost, host.docker.internal, or container name)
    private static final String JDBC_URL = "jdbc:clickhouse://localhost:8123/zeotap?user=default";  // Use host.docker.internal if inside Docker

    private static ClickHouseDataSource dataSource;

    static {
        try {
            dataSource = new ClickHouseDataSource(JDBC_URL);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public static ClickHouseDataSource getDataSource() {
        return dataSource;
    }
}
