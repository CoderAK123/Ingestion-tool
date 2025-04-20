package com.zeotap.service;

import com.clickhouse.jdbc.ClickHouseConnection;
import com.clickhouse.jdbc.ClickHouseResultSet;
import com.clickhouse.jdbc.ClickHouseStatement;
import com.zeotap.dto.ClickHouseConfigDTO;
import com.zeotap.dto.ClickHouseTableRequest;
import com.zeotap.dto.IngestionRequest;
import org.springframework.stereotype.Service;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.io.ByteArrayOutputStream;

import org.springframework.web.multipart.MultipartFile;
import com.clickhouse.jdbc.ClickHouseConnection;
import java.io.FileWriter;
import java.sql.*;
import java.util.*;

@Service
public class ClickHouseService {

    // Create JDBC connection
    private Connection createConnection(ClickHouseConfigDTO config) throws Exception {
        String url = String.format("jdbc:clickhouse://%s:%d/%s", config.getHost(), config.getPort(), config.getDatabase());
        Properties props = new Properties();
        props.setProperty("user", config.getUsername());
        props.setProperty("password", config.getPassword());
        props.setProperty("ssl", config.isUseHttps() ? "true" : "false");
        return DriverManager.getConnection(url, props);
    }

    // 1. Test ClickHouse connection
    public boolean testConnection(ClickHouseConfigDTO config) {
        try (Connection conn = createConnection(config)) {
            return conn != null && !conn.isClosed();
        } catch (Exception e) {
            e.printStackTrace();
            return false;
        }
    }

    // 2. Get table names from ClickHouse
    public List<String> fetchTableNames(ClickHouseConfigDTO config) {
        if (config == null || config.getDatabase() == null) {
            throw new IllegalArgumentException("Database name is required.");
        }
    
       
        List<String> tableNames = new ArrayList<>();
        System.out.println("🔧 Connecting to fetch tables for DB: " + config.getDatabase());
        System.out.println("🔧 Using connection details: " +
                "Host: " + config.getHost() + ", Port: " + config.getPort() +
                ", Database: " + config.getDatabase());
    
        try (Connection conn = createConnection(config);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SHOW TABLES FROM " + config.getDatabase())) {
    
            while (rs.next()) {
                String table = rs.getString(1);
                System.out.println("📋 Found table: " + table);
                tableNames.add(table);
            }
    
        } catch (Exception e) {
            System.out.println("❌ Error fetching tables:");
            e.printStackTrace();
        }
    
        System.out.println("✅ Tables found: " + tableNames.size());
        return tableNames;
    }
    
    

    // 3. Get column names from a table
    public List<String> fetchColumns(ClickHouseTableRequest request) {
        List<String> columnNames = new ArrayList<>();
        ClickHouseConfigDTO config = request.getConfig();
    
        try (Connection conn = createConnection(config)) {
            String sql = "SELECT * FROM " + request.getTableName() + " LIMIT 1";
            System.out.println("Executing SQL: " + sql);
            
            PreparedStatement stmt = conn.prepareStatement(sql);
            ResultSet rs = stmt.executeQuery();
    
            ResultSetMetaData metaData = rs.getMetaData();
            for (int i = 1; i <= metaData.getColumnCount(); i++) {
                columnNames.add(metaData.getColumnName(i));
            }
        } catch (Exception e) {
            e.printStackTrace(); // 👈 This is important to see any error
        }
    
        return columnNames;
    }

    

    // 4. Export ClickHouse table to CSV
    public int ingestToCSV(IngestionRequest request, ByteArrayOutputStream outputStream) {
        int rowCount = 0;
    
        ClickHouseConfigDTO config = request.getConfig();
        String tableName = request.getTableName();
        String delimiter = request.getDelimiter();
        String joinQuery = request.getJoinQuery(); // ✅ support for JOIN
    
        List<String> selectedColumns = request.getColumns();
        String columnQueryPart = (selectedColumns != null && !selectedColumns.isEmpty())
                ? String.join(", ", selectedColumns)
                : "*";
    
        String finalQuery;
    
        if (joinQuery != null && !joinQuery.isBlank()) {
            finalQuery = joinQuery; // ✅ use full join query if provided
        } else {
            finalQuery = "SELECT " + columnQueryPart + " FROM " + tableName;
        }
    
        try (Connection conn = createConnection(config);
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(finalQuery)) {
    
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
    
            // Write CSV header
            for (int i = 1; i <= columnCount; i++) {
                outputStream.write(metaData.getColumnName(i).getBytes(StandardCharsets.UTF_8));
                if (i < columnCount) outputStream.write(delimiter.getBytes(StandardCharsets.UTF_8));
            }
            outputStream.write("\n".getBytes(StandardCharsets.UTF_8));
    
            // Write CSV rows
            while (rs.next()) {
                for (int i = 1; i <= columnCount; i++) {
                    String value = rs.getString(i);
                    outputStream.write((value != null ? value : "").getBytes(StandardCharsets.UTF_8));
                    if (i < columnCount) outputStream.write(delimiter.getBytes(StandardCharsets.UTF_8));
                }
                outputStream.write("\n".getBytes(StandardCharsets.UTF_8));
                rowCount++;
            }
    
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("CSV export failed: " + e.getMessage());
        }
    
        return rowCount;
    }
    
    public int ingestFromCSV(MultipartFile file, ClickHouseTableRequest request) throws Exception {
        ClickHouseConfigDTO config = request.getConfig();
        int inserted = 0;
    
        try (Connection conn = createConnection(config);
             PreparedStatement stmt = conn.prepareStatement(generateInsertSQL(file, request.getTableName()))) {
    
            BufferedReader reader = new BufferedReader(new InputStreamReader(file.getInputStream()));
            String headerLine = reader.readLine();
            if (headerLine == null) return 0;
    
            String[] columns = headerLine.split(",");
            String line;
    
            while ((line = reader.readLine()) != null) {
                String[] values = line.split(",");
                for (int i = 0; i < values.length; i++) {
                    stmt.setString(i + 1, values[i]);
                }
                stmt.addBatch();
                inserted++;
            }
            stmt.executeBatch();
        }
    
        return inserted;
    }
    private String generateInsertSQL(MultipartFile file, String tableName) throws Exception {
        BufferedReader reader = new BufferedReader(new InputStreamReader(file.getInputStream()));
        String headerLine = reader.readLine();
        if (headerLine == null) throw new Exception("Empty CSV file");
    
        String[] columns = headerLine.split(",");
        StringBuilder sql = new StringBuilder("INSERT INTO ");
        sql.append(tableName).append(" (");
        sql.append(String.join(",", columns));
        sql.append(") VALUES (");
        sql.append("?,".repeat(columns.length));
        sql.setLength(sql.length() - 1); // remove trailing comma
        sql.append(")");
        return sql.toString();
    }
    public List<Map<String, Object>> previewTable(IngestionRequest request) {
        List<Map<String, Object>> result = new ArrayList<>();
        
        String joinQuery = request.getJoinQuery(); // Get the JOIN query if provided
        String tableName = request.getTableName();
        List<String> selectedColumns = request.getColumns(); // Columns from frontend
    
        String columnQueryPart = (selectedColumns != null && !selectedColumns.isEmpty())
                ? String.join(", ", selectedColumns)
                : "*";
    
        String finalQuery;
    
        // If joinQuery is provided, use it; otherwise, default to a basic SELECT query
        if (joinQuery != null && !joinQuery.isBlank()) {
            finalQuery = joinQuery; // Use the provided JOIN query
        } else {
            finalQuery = "SELECT " + columnQueryPart + " FROM " + tableName + " LIMIT 10"; // Preview the first 10 rows
        }
    
        try (Connection conn = createConnection(request.getConfig());
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(finalQuery)) {
    
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
    
            while (rs.next()) {
                Map<String, Object> rowData = new HashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    rowData.put(metaData.getColumnName(i), rs.getObject(i));
                }
                result.add(rowData); // Add row to the result list
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException("Preview failed: " + e.getMessage());
        }
    
        return result; // Return the preview data (list of maps)
    }
    
    
    
}
