package com.zeotap.controller;

import com.zeotap.dto.ClickHouseConfigDTO;
import com.zeotap.dto.ClickHouseTableRequest;
import com.zeotap.dto.IngestionRequest;
import com.zeotap.service.ClickHouseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ContentDisposition;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import java.io.ByteArrayOutputStream;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

@RestController
@CrossOrigin(origins ={ "http://localhost:3000","http://127.0.0.1:5500"})
@RequestMapping("/api/clickhouse")
public class ClickHouseController {

    @Autowired
    private ClickHouseService clickHouseService;

    @PostMapping("/connect")
    public ResponseEntity<Boolean> connectToClickHouse(@RequestBody ClickHouseConfigDTO config) {
        boolean connected = clickHouseService.testConnection(config);
        return ResponseEntity.ok(connected);
    }
    

    @PostMapping("/tables")
    public ResponseEntity<List<String>> getTables(@RequestBody ClickHouseConfigDTO config) {
        try {
            List<String> tableNames = clickHouseService.fetchTableNames(config);
            System.out.println("Fetched Tables: " + tableNames);  // Log the fetched tables
            return ResponseEntity.ok(tableNames);
        } catch (Exception e) {
            System.err.println("Error fetching tables: " + e.getMessage());
            return ResponseEntity.status(HttpStatus.BAD_REQUEST).body(null);
        }
    }

    @PostMapping("/columns")
    public ResponseEntity<List<String>> getColumns(@RequestBody ClickHouseTableRequest request) {
        List<String> columns = clickHouseService.fetchColumns(request);
        return ResponseEntity.ok(columns);
    }

    @PostMapping("/ingest-to-file")
    public ResponseEntity<byte[]> downloadCSV(@RequestBody IngestionRequest ingestionRequest) {
        try {
            ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
            int rowCount = clickHouseService.ingestToCSV(ingestionRequest, outputStream);
    
            byte[] csvBytes = outputStream.toByteArray();
    
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);
            headers.setContentDisposition(ContentDisposition.attachment()
                .filename(ingestionRequest.getTableName() + ".csv").build());
    
            return new ResponseEntity<>(csvBytes, headers, HttpStatus.OK);
        } catch (Exception e) {
            e.printStackTrace();
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(("Error generating CSV file: " + e.getMessage()).getBytes());
        }
    }
    

    @PostMapping("/upload-csv")
public ResponseEntity<String> uploadCsvToClickHouse(
        @RequestPart("file") MultipartFile file,
        @RequestPart("config") ClickHouseTableRequest request) {
    try {
        int inserted = clickHouseService.ingestFromCSV(file, request);
        return ResponseEntity.ok("Inserted " + inserted + " rows into " + request.getTableName());
    } catch (Exception e) {
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Upload failed: " + e.getMessage());
    }
}
@PostMapping("/preview")
public ResponseEntity<List<Map<String, Object>>> previewTable(@RequestBody IngestionRequest request) {
    try {
        // Call the service to get the preview data
        List<Map<String, Object>> data = clickHouseService.previewTable(request);
        
        // Return the data in the response
        return ResponseEntity.ok(data);
    } catch (Exception e) {
        // If there's an error, return INTERNAL_SERVER_ERROR
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(null);
    }
}



}
