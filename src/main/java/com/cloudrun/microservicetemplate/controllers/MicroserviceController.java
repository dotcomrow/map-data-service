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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.cloudrun.microservicetemplate.config.BigQueryConfiguration.BigQueryFileGateway;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.StandardSQLTypeName;
import com.google.cloud.spring.bigquery.core.BigQueryTemplate;
import com.google.cloud.spring.bigquery.core.WriteApiResponse;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;
import org.springframework.web.servlet.ModelAndView;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

/** Example REST controller to demonstrate structured logging. */
@RestController
public class MicroserviceController {
  // 'spring-cloud-gcp-starter-logging' module provides support for
  // associating a web request trace ID with the corresponding log entries.
  // https://cloud.spring.io/spring-cloud-gcp/multi/multi__stackdriver_logging.html
  private static final Logger logger = LoggerFactory.getLogger(MicroserviceController.class);

  private final BigQueryFileGateway bigQueryFileGateway;

  private final BigQueryTemplate bigQueryTemplate;

  @Value("${bigquery.table.name:#{null}}")
  private String tableName;

  public MicroserviceController(
      BigQueryFileGateway bigQueryFileGateway, BigQueryTemplate bigQueryTemplate) {
    this.bigQueryFileGateway = bigQueryFileGateway;
    this.bigQueryTemplate = bigQueryTemplate;
  }

  @GetMapping("/map-data")
  public @ResponseBody String index() throws JsonProcessingException {
    // // Example of structured logging - add custom fields
    // MDC.put("logField", "custom-entry");
    // MDC.put("arbitraryField", "custom-entry");
    // // Use logger with log correlation
    // // https://cloud.google.com/run/docs/logging#correlate-logs
    // logger.info("Structured logging example.");
    Object user=SecurityContextHolder.getContext().getAuthentication().getPrincipal();
    ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
    String json = ow.writeValueAsString(user);
    return json;
  }

  @PostMapping("/map-data")
  public ResponseEntity<Void> handleJsonTextUpload(
      @RequestParam("jsonRows") String jsonRows) {
      try {
      writeApiRes =
          this.bigQueryTemplate.writeJsonStream(
              tableName, new ByteArrayInputStream(jsonRows.getBytes()));
      } catch (IOException e) {
        logger.error("Error writing to BigQuery", e);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
      }
    return ResponseEntity.created().build();
  }
}
