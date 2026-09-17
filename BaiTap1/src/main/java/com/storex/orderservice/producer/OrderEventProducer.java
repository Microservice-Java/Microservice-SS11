package com.storex.orderservice.producer;

import com.storex.orderservice.model.OrderEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.util.concurrent.CompletableFuture;

@Service
public class OrderEventProducer {

    private static final Logger log = LoggerFactory.getLogger(OrderEventProducer.class);
    private static final String TOPIC = "order-events";

    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @Autowired
    public OrderEventProducer(KafkaTemplate<String, OrderEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public Mono<SendResult<String, OrderEvent>> sendOrderEvent(OrderEvent orderEvent) {
        log.info("Gửi OrderEvent tới Kafka topic '{}': orderId={}, productName={}",
                TOPIC, orderEvent.getOrderId(), orderEvent.getProductName());

        CompletableFuture<SendResult<String, OrderEvent>> future =
                kafkaTemplate.send(TOPIC, orderEvent.getOrderId(), orderEvent);

        return Mono.fromFuture(future)
                .doOnSuccess(result -> log.info("Gửi OrderEvent thành công tới Kafka topic '{}', partition: {}, offset: {}",
                        TOPIC, result.getRecordMetadata().partition(), result.getRecordMetadata().offset()))
                .doOnError(ex -> log.error("Lỗi khi gửi OrderEvent tới Kafka topic '{}': {}", TOPIC, ex.getMessage(), ex));
    }
}
