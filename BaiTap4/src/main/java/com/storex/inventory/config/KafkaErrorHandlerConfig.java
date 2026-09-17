package com.storex.inventory.config;

import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaErrorHandlerConfig {

    private static final Logger log = LoggerFactory.getLogger(KafkaErrorHandlerConfig.class);
    public static final String DLQ_TOPIC = "storex-order-events.DLQ";

    @Bean
    public DefaultErrorHandler errorHandler(KafkaTemplate<Object, Object> kafkaTemplate) {
        // DeadLetterPublishingRecoverer tự động đẩy message thất bại sang Dead Letter Topic (DLQ): storex-order-events.DLQ
        DeadLetterPublishingRecoverer recoverer = new DeadLetterPublishingRecoverer(kafkaTemplate, (record, exception) -> {
            // REQ-02 REQUIREMENT: In log mức ERROR thông báo khi ném tin nhắn vào DLQ
            log.error("Đã ném đơn hàng bị lỗi vào DLQ: Key={}, Offset={}, Target Topic={}. Nguyên nhân: {}",
                    record.key(), record.offset(), DLQ_TOPIC, exception.getMessage());
            return new TopicPartition(DLQ_TOPIC, record.partition());
        });

        // Thử lại (Retry) tối đa 3 lần, mỗi lần cách nhau 2 giây (FixedBackOff: 2000ms delay, max 2 retries)
        FixedBackOff backOff = new FixedBackOff(2000L, 2L);

        DefaultErrorHandler errorHandler = new DefaultErrorHandler(recoverer, backOff);
        errorHandler.setRetryListeners((record, ex, deliveryAttempt) ->
                log.warn("Retry lượt thứ {}/3 (cách 2s) cho order key: {}, offset: {} do lỗi: {}",
                        deliveryAttempt, record.key(), record.offset(), ex.getMessage())
        );

        return errorHandler;
    }
}
