package com.dremio.service.reflection.analysis;

import com.dremio.exec.util.ViewFieldsHelper;
import com.dremio.service.namespace.dataset.proto.DatasetConfig;
import com.dremio.service.namespace.dataset.proto.ViewFieldType;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AutonomousReflectionClient {
  private static final Logger logger = LoggerFactory.getLogger(AutonomousReflectionClient.class);
  private static final String AI_SERVICE_URL = "http://localhost:8000/predict/schema";
  private static final ObjectMapper mapper = new ObjectMapper();

  public static class AIResponse {
    public List<String> dimensions;
    public List<String> measures;
  }

  public static AIResponse getRecommendations(DatasetConfig datasetConfig) {
    try {
      Map<String, Object> payload = new HashMap<>();
      payload.put("datasetPath", datasetConfig.getFullPathList());

      List<Map<String, String>> columns = new ArrayList<>();
      List<ViewFieldType> viewFields = ViewFieldsHelper.getViewFields(datasetConfig);
      if (viewFields != null) {
        for (ViewFieldType field : viewFields) {
          Map<String, String> col = new HashMap<>();
          col.put("name", field.getName());
          col.put("type", field.getType());
          columns.add(col);
        }
      }
      payload.put("columns", columns);

      String jsonPayload = mapper.writeValueAsString(payload);
      logger.info("Requesting autonomous reflection for dataset, payload: {}", jsonPayload);

      URL url = new URL(AI_SERVICE_URL);
      HttpURLConnection conn = (HttpURLConnection) url.openConnection();
      conn.setRequestMethod("POST");
      conn.setRequestProperty("Content-Type", "application/json; utf-8");
      conn.setRequestProperty("Accept", "application/json");
      conn.setConnectTimeout(3000); // 3 seconds timeout
      conn.setReadTimeout(5000); // 5 seconds timeout
      conn.setDoOutput(true);

      try (OutputStream os = conn.getOutputStream()) {
        byte[] input = jsonPayload.getBytes("utf-8");
        os.write(input, 0, input.length);
      }

      int responseCode = conn.getResponseCode();
      if (responseCode == 200) {
        try (BufferedReader br =
            new BufferedReader(new InputStreamReader(conn.getInputStream(), "utf-8"))) {
          StringBuilder response = new StringBuilder();
          String responseLine = null;
          while ((responseLine = br.readLine()) != null) {
            response.append(responseLine.trim());
          }
          AIResponse aiResponse = mapper.readValue(response.toString(), AIResponse.class);
          logger.info("Received AI reflection recommendations successfully.");
          return aiResponse;
        }
      } else {
        logger.warn("AI Service returned code: {}", responseCode);
      }
    } catch (Exception e) {
      logger.warn("Failed to query autonomous reflection service (fallback to heuristic): {}", e.getMessage());
    }
    return null;
  }
}
