package com.lab.ycsb;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.*;
import site.ycsb.ByteIterator;
import site.ycsb.DB;
import site.ycsb.DBException;
import site.ycsb.Status;
import site.ycsb.StringByteIterator;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.TimeUnit;

/**
 * YCSB binding for the custom FastAPI key-value store.
 *
 * This class translates YCSB operations (read, insert, update, delete)
 * into HTTP requests to the FastAPI coordinator.
 */
public class FastApiDbClient extends DB {
    private static final MediaType JSON = MediaType.get("application/json; charset=utf-8");

    private OkHttpClient httpClient;
    private ObjectMapper jsonMapper;
    private String coordinatorUrl;

    @Override
    public void init() throws DBException {
        // Initialize HTTP client
        this.httpClient = new OkHttpClient.Builder()
                .connectTimeout(10, TimeUnit.SECONDS)
                .writeTimeout(10, TimeUnit.SECONDS)
                .readTimeout(30, TimeUnit.SECONDS)
                .build();

        // Initialize JSON mapper
        this.jsonMapper = new ObjectMapper();

        // Get coordinator URL from properties (passed via -p)
        Properties props = getProperties();
        this.coordinatorUrl = props.getProperty("coordinator.url", "http://localhost:8000");
        System.out.println("FastApiDbClient initialized. Coordinator URL: " + this.coordinatorUrl);
    }

    @Override
    public void cleanup() {
        // Clean up resources (if any)
        System.out.println("Cleaning up FastApiDbClient.");
    }
    
    /**
     * Helper to convert YCSB's ByteIterator to a standard Map.
     */
    private Map<String, String> byteIteratorToMap(Map<String, ByteIterator> values) {
        Map<String, String> map = new HashMap<>();
        for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
            map.put(entry.getKey(), entry.getValue().toString());
        }
        return map;
    }

    @Override
    public Status read(String table, String key, Set<String> fields, Map<String, ByteIterator> result) {
        String url = this.coordinatorUrl + "/read/" + table + "/" + key;
        Request request = new Request.Builder().url(url).get().build();

        try (Response response = httpClient.newCall(request).execute()) {
            if (response.isSuccessful()) {
                // Parse the JSON response
                String responseBody = response.body().string();
                
                // We need to simulate field retrieval
                Map<String, Object> respMap = jsonMapper.readValue(responseBody, new TypeReference<Map<String, Object>>() {});
                
                // This is the data we want: e.g., {"name":"Alice"} or {"data":"version_2_FINAL"}
                Object rawResponse = respMap.get("response");
                
                if (rawResponse instanceof Map) {
                    Map<String, Object> data = (Map<String, Object>) rawResponse;

                    // --- FIX ---
                    // Manually iterate and populate the result map
                    // This correctly handles Maps like {"name": "Alice", "age": 25}
                    // It converts all values to StringByteIterator
                    for (Map.Entry<String, Object> entry : data.entrySet()) {
                        if (entry.getValue() != null) {
                            result.put(entry.getKey(), new StringByteIterator(entry.getValue().toString()));
                        }
                    }
                    // --- END FIX ---
                    
                } else if (rawResponse != null) {
                    // Fallback for simple string values (if API changes)
                    result.put("field0", new StringByteIterator(rawResponse.toString()));
                }

                return Status.OK;
            } else if (response.code() == 404) {
                return Status.NOT_FOUND;
            } else {
                System.err.println("Read failed: " + response.code() + " " + response.message());
                return Status.ERROR;
            }
        } catch (IOException e) {
            System.err.println("Read exception: " + e.getMessage());
            e.printStackTrace();
            return Status.ERROR;
        }
    }

    @Override
    public Status insert(String table, String key, Map<String, ByteIterator> values) {
        String url = this.coordinatorUrl + "/create";
        
        // Build the payload
        // {"table": "...", "key": "...", "value": {...}}
        Map<String, Object> payload = new HashMap<>();
        payload.put("table", table);
        payload.put("key", key);
        payload.put("value", byteIteratorToMap(values)); // "value" is the map of fields

        try {
            String jsonPayload = jsonMapper.writeValueAsString(payload);
            RequestBody body = RequestBody.create(jsonPayload, JSON);
            Request request = new Request.Builder().url(url).post(body).build();
            
            try (Response response = httpClient.newCall(request).execute()) {
                if (response.isSuccessful()) {
                    return Status.OK;
                } else {
                    System.err.println("Insert failed: " + response.code() + " " + response.body().string());
                    return Status.ERROR;
                }
            }
        } catch (Exception e) {
            System.err.println("Insert exception: " + e.getMessage());
            e.printStackTrace();
            return Status.ERROR;
        }
    }

    @Override
    public Status update(String table, String key, Map<String, ByteIterator> values) {
        String url = this.coordinatorUrl + "/update";

        // Build the payload
        // {"table": "...", "key": "...", "value": {...}}
        Map<String, Object> payload = new HashMap<>();
        payload.put("table", table);
        payload.put("key", key);
        payload.put("value", byteIteratorToMap(values)); // "value" is the map of fields

        try {
            String jsonPayload = jsonMapper.writeValueAsString(payload);
            RequestBody body = RequestBody.create(jsonPayload, JSON);
            Request request = new Request.Builder().url(url).put(body).build();
            
            try (Response response = httpClient.newCall(request).execute()) {
                if (response.isSuccessful()) {
                    return Status.OK;
                } else {
                     System.err.println("Update failed: " + response.code() + " " + response.body().string());
                    return Status.ERROR;
                }
            }
        } catch (Exception e) {
             System.err.println("Update exception: " + e.getMessage());
             e.printStackTrace();
            return Status.ERROR;
        }
    }
    
    @Override
    public Status delete(String table, String key) {
        String url = this.coordinatorUrl + "/delete/" + table + "/" + key;
        Request request = new Request.Builder().url(url).delete().build();

        try (Response response = httpClient.newCall(request).execute()) {
            if (response.isSuccessful() || response.code() == 204) {
                return Status.OK;
            } else if (response.code() == 404) {
                return Status.NOT_FOUND;
            } else {
                System.err.println("Delete failed: " + response.code() + " " + response.message());
                return Status.ERROR;
            }
        } catch (IOException e) {
            System.err.println("Delete exception: " + e.getMessage());
            e.printStackTrace();
            return Status.ERROR;
        }
    }

    @Override
    public Status scan(String table, String startkey, int recordcount, Set<String> fields,
                       Vector<HashMap<String, ByteIterator>> result) {
        // Scan is not implemented in our simple K/V store
        return Status.NOT_IMPLEMENTED;
    }
}