package com.sraavan.kafkaguard.config;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sraavan.kafkaguard.policy.PolicyCodec;
import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.info.Info;
import io.swagger.v3.oas.models.info.License;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class AppConfig {

    @Bean
    public PolicyCodec policyCodec(ObjectMapper mapper) {
        return new PolicyCodec(mapper);
    }

    @Bean
    public OpenAPI kafkaGuardOpenApi() {
        return new OpenAPI().info(new Info()
                .title("kafka-guard API")
                .version("0.1.0")
                .description("A governance gateway for Apache Kafka topic provisioning: "
                        + "enforce naming, partitioning, retention, and ownership policy.")
                .license(new License().name("MIT")));
    }
}
