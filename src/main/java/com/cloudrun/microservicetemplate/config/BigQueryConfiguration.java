package com.cloudrun.microservicetemplate.config;

import com.google.cloud.bigquery.FormatOptions;
import com.google.cloud.bigquery.Job;
import com.google.cloud.spring.bigquery.core.BigQueryTemplate;
import com.google.cloud.spring.bigquery.integration.BigQuerySpringMessageHeaders;
import com.google.cloud.spring.bigquery.integration.outbound.BigQueryFileMessageHandler;
import java.util.concurrent.CompletableFuture;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.integration.annotation.MessagingGateway;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.gateway.GatewayProxyFactoryBean;
import org.springframework.messaging.MessageHandler;
import org.springframework.messaging.handler.annotation.Header;

@Configuration
public class BigQueryConfiguration {

  @Bean
  public DirectChannel bigQueryWriteDataChannel() {
    return new DirectChannel();
  }

  @Bean
  public DirectChannel bigQueryJobReplyChannel() {
    return new DirectChannel();
  }

  @Bean
  @ServiceActivator(inputChannel = "bigQueryWriteDataChannel")
  public MessageHandler messageSender(BigQueryTemplate bigQueryTemplate) {
    BigQueryFileMessageHandler messageHandler = new BigQueryFileMessageHandler(bigQueryTemplate);
    messageHandler.setFormatOptions(FormatOptions.csv());
    messageHandler.setOutputChannel(bigQueryJobReplyChannel());
    return messageHandler;
  }

  @Primary
  @Bean
  public GatewayProxyFactoryBean gatewayProxyFactoryBean() {
    GatewayProxyFactoryBean factoryBean = new GatewayProxyFactoryBean(BigQueryFileGateway.class);
    factoryBean.setDefaultRequestChannel(bigQueryWriteDataChannel());
    factoryBean.setDefaultReplyChannel(bigQueryJobReplyChannel());
    // Ensures that BigQueryFileGateway does not return double-wrapped CompletableFutures
    factoryBean.setAsyncExecutor(null);
    return factoryBean;
  }

  /** Spring Integration gateway which allows sending data to load to BigQuery through a channel. */
  @MessagingGateway
  public interface BigQueryFileGateway {
    CompletableFuture<Job> writeToBigQueryTable(
        byte[] csvData, @Header(BigQuerySpringMessageHeaders.TABLE_NAME) String tableName);
  }
}
