package com.storex.producer.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    public static final String TOPIC_NAME = "storex-order-events";

    @Bean
    public NewTopic storexOrderEventsTopic() {
        // Cấu hình topic storex-order-events có 5 Partitions
        return TopicBuilder.name(TOPIC_NAME)
                .partitions(5)
                .replicas(1)
                .build();
    }
}
