/*
 * Copyright (C) 2017-2019 Dremio Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
import java.nio.charset.StandardCharsets; // Thêm import này
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.slf4j.Logger;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.slf4j.LoggerFactory;

public class AutonomousReflectionClient {
  private static final Logger logger = LoggerFactory.getLogger(AutonomousReflectionClient.class);
  private static final String AI_SERVICE_URL = "http://172.23.176.1:8000/predict/schema"; //may be need to change depend on PCs
  private static final ObjectMapper mapper = new ObjectMapper();

@JsonIgnoreProperties(ignoreUnknown = true)
 public static class AIResponse {
    public List<String> datasetPath; // Thêm trường này
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
      conn.setRequestProperty("Content-Type", "application/json; charset=utf-8");
      conn.setRequestProperty("Accept", "application/json");
      conn.setConnectTimeout(3000); 
      conn.setReadTimeout(5000); 
      conn.setDoOutput(true);

      try (OutputStream os = conn.getOutputStream()) {
        // SỬA TẠI ĐÂY: Dùng StandardCharsets.UTF_8 thay vì "utf-8"
        byte[] input = jsonPayload.getBytes(StandardCharsets.UTF_8);
        os.write(input, 0, input.length);
      }

      int responseCode = conn.getResponseCode();
      if (responseCode == 200) {
        try (BufferedReader br =
            new BufferedReader(new InputStreamReader(conn.getInputStream(), StandardCharsets.UTF_8))) {
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