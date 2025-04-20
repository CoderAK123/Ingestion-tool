// src/main/java/com/zeotap/config/ClickHouseConfig.java
package com.zeotap.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import com.clickhouse.jdbc.ClickHouseDataSource;

import java.sql.SQLException;
import java.util.Properties;

@Configuration
public class ClickHouseConfig {

    @Bean
    public ClickHouseDataSource clickHouseDataSource() throws SQLException {
        Properties properties = new Properties();
        properties.setProperty("user", "default"); // No password

        // Adjust this if your container isn't on localhost or if port is different
        String url = "jdbc:clickhouse://host.docker.internal:9000/zeotap";
        return new ClickHouseDataSource(url, properties);
    }
}
