package com.zeotap.controller;

import com.zeotap.dto.ClickHouseConfigDTO;
import com.zeotap.dto.ClickHouseTableRequest;
import com.zeotap.dto.IngestionRequest;
import com.zeotap.service.ClickHouseService;
import com.zeotap.service.FlatFileIngestionService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;
import java.io.ByteArrayOutputStream;


import java.util.List;

@RestController
@RequestMapping("/api")
@CrossOrigin(origins = "*")
public class IngestionController {

    @Autowired
    private ClickHouseService clickHouseService;

    @Autowired
    private FlatFileIngestionService flatFileService;
    ByteArrayOutputStream outputStream = new ByteArrayOutputStream();


    // 1. Test ClickHouse Connection
    @PostMapping("/test-connection")
    public boolean testClickHouseConnection(@RequestBody ClickHouseConfigDTO config) {
        return clickHouseService.testConnection(config);
    }

    // 2. Get list of tables from ClickHouse
    @PostMapping("/tables")
    public List<String> getClickHouseTables(@RequestBody ClickHouseConfigDTO config) {
        return clickHouseService.fetchTableNames(config);
    }

    // 3. Get column list from a specific table
    @PostMapping("/columns")
    public List<String> getColumns(@RequestBody ClickHouseTableRequest request) {
        return clickHouseService.fetchColumns(request);
    }

    // 4. Ingest ClickHouse ➜ Flat File
    @PostMapping("/clickhouse-to-csv")
    public int ingestToCSV(@RequestBody IngestionRequest request) {
        return clickHouseService.ingestToCSV(request,outputStream);
    }

    // 5. Ingest Flat File ➜ ClickHouse
    @PostMapping("/csv-to-clickhouse")
    public int ingestCSVToClickHouse(@RequestBody IngestionRequest request) {
        return flatFileService.ingestCSVToClickHouse(
                request.getConfig(),
                request.getTableName(),
                request.getOutputFilePath(),
                request.getDelimiter()
        );
    }
}
