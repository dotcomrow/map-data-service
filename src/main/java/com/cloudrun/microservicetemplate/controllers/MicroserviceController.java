/*
 * Copyright 2021 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.cloudrun.microservicetemplate.controllers;

import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.cloud.bigquery.BigQuery;
import com.google.cloud.bigquery.FieldValue;
import com.google.cloud.bigquery.FieldValueList;
import com.google.cloud.bigquery.QueryJobConfiguration;
@RestController
public class MicroserviceController {
  // 'spring-cloud-gcp-starter-logging' module provides support for
  // associating a web request trace ID with the corresponding log entries.
  // https://cloud.spring.io/spring-cloud-gcp/multi/multi__stackdriver_logging.html
  private static final Logger logger = LoggerFactory.getLogger(MicroserviceController.class);

  @Autowired
  BigQuery bigquery;

  @GetMapping("/map-data")
  ResponseEntity<Map<String, String>> index() throws JsonProcessingException {
    // // Example of structured logging - add custom fields
    // MDC.put("logField", "custom-entry");
    // MDC.put("arbitraryField", "custom-entry");
    // // Use logger with log correlation
    // // https://cloud.google.com/run/docs/logging#correlate-logs
    // logger.info("Structured logging example.");
    // Object user=SecurityContextHolder.getContext().getAuthentication().getPrincipal();
    // ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
    // String json = ow.writeValueAsString(user);
    // return json;
    Map<String, String> map = new HashMap<>();
    try {
          String query = "SELECT * FROM mapData.location_data;";
          QueryJobConfiguration queryConfig =
            QueryJobConfiguration.newBuilder(query).build();

          // Run the query using the BigQuery object
          for (FieldValueList row : bigquery.query(queryConfig).iterateAll()) {
            for (FieldValue val : row) {
              map.put("column", val.getStringValue());
            }
          }
      } catch (InterruptedException e) {
        logger.error("Error writing to BigQuery", e);
        return ResponseEntity.status(HttpStatus.SC_INTERNAL_SERVER_ERROR).build();
      }
      return ResponseEntity.ok().body(map);
  }

  // @PostMapping("/map-data")
  // public ResponseEntity<Void> handleJsonTextUpload(
  //     @RequestParam("jsonRows") String jsonRows) {
  //     try {
  //         String query = "SELECT column FROM table;";
  //         QueryJobConfiguration queryConfig =
  //           QueryJobConfiguration.newBuilder(query).build();

  //         // Run the query using the BigQuery object
  //         for (FieldValueList row : bigquery.query(queryConfig).iterateAll()) {
  //           for (FieldValue val : row) {
  //             System.out.println(val);
  //           }
  //         }
  //     } catch (InterruptedException e) {
  //       logger.error("Error writing to BigQuery", e);
  //       return ResponseEntity.status(HttpStatus.SC_INTERNAL_SERVER_ERROR).build();
  //     }
  //   return ResponseEntity.ok().build();
  // }
}
