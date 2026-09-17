package com.storex.producer.service;

import com.storex.producer.config.KafkaTopicConfig;
import com.storex.producer.model.dto.OrderRequest;
import com.storex.producer.model.event.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

@Service
public class OrderEventProducerService {

    private static final Logger log = LoggerFactory.getLogger(OrderEventProducerService.class);
    private final KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    @Autowired
    public OrderEventProducerService(KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public Mono<String> processAndPublishOrder(OrderRequest request) {
        // Sinh mã orderId ngẫu nhiên
        String orderId = "ORD-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();

        OrderCreatedEvent event = OrderCreatedEvent.builder()
                .orderId(orderId)
                .userId(request.getUserId())
                .productName(request.getProductName())
                .quantity(request.getQuantity())
                .totalAmount(request.getTotalAmount())
                .status("CREATED")
                .eventType("order.created")
                .createdAt(LocalDateTime.now())
                .build();

        log.info("Khởi tạo đơn hàng {}: Bắt đầu đẩy sự kiện order.created vào Kafka topic '{}'...",
                orderId, KafkaTopicConfig.TOPIC_NAME);

        // BUG-03 FIX: BẮT BUỘC TRUYỀN orderId LÀM KHÓA (KEY) PHÂN TUYẾN KAFKA PARTITION
        // Tất cả sự kiện của cùng 1 orderId luôn luôn được định tuyến vào cùng 1 Partition trong 5 Partitions
        CompletableFuture<SendResult<String, OrderCreatedEvent>> future =
                kafkaTemplate.send(KafkaTopicConfig.TOPIC_NAME, orderId, event);

        return Mono.fromFuture(future)
                .doOnSuccess(result -> log.info("Đẩy sự kiện thành công! OrderId: {}, Partition: {}, Offset: {}",
                        orderId, result.getRecordMetadata().partition(), result.getRecordMetadata().offset()))
                .doOnError(ex -> log.error("Lỗi khi đẩy sự kiện OrderId {}: {}", orderId, ex.getMessage(), ex))
                .thenReturn(orderId);
    }
}
