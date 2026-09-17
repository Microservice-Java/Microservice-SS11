package com.storex.producer.service;

import com.storex.producer.config.KafkaTopicConfig;
import com.storex.producer.model.dto.OrderRequest;
import com.storex.producer.model.event.OrderCreatedEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OrderEventProducerServiceTest {

    @Mock
    private KafkaTemplate<String, OrderCreatedEvent> kafkaTemplate;

    @InjectMocks
    private OrderEventProducerService orderEventProducerService;

    private OrderRequest mockRequest;

    @BeforeEach
    void setUp() {
        mockRequest = OrderRequest.builder()
                .userId(101L)
                .productName("MacBook Pro M3 Max")
                .quantity(1)
                .totalAmount(3499.0)
                .build();
    }

    @Test
    @DisplayName("Test BUG-03: Kiểm tra orderId được truyền làm Partition Key trong kafkaTemplate.send()")
    void processAndPublishOrder_UsesOrderIdAsPartitionKey() {
        SendResult<String, OrderCreatedEvent> mockResult = mock(SendResult.class, RETURNS_DEEP_STUBS);
        when(kafkaTemplate.send(eq(KafkaTopicConfig.TOPIC_NAME), anyString(), any(OrderCreatedEvent.class)))
                .thenReturn(CompletableFuture.completedFuture(mockResult));

        Mono<String> resultMono = orderEventProducerService.processAndPublishOrder(mockRequest);

        StepVerifier.create(resultMono)
                .expectNextMatches(orderId -> orderId != null && orderId.startsWith("ORD-"))
                .verifyComplete();

        // Kiểm tra lệnh send được gọi đúng tham số topic "storex-order-events" và key khác null
        verify(kafkaTemplate, times(1)).send(eq("storex-order-events"), argThat(key -> key != null && key.startsWith("ORD-")), any(OrderCreatedEvent.class));
    }
}
