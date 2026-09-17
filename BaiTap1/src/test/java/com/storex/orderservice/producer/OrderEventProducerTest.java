package com.storex.orderservice.producer;

import com.storex.orderservice.model.OrderEvent;
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

import java.time.LocalDateTime;
import java.util.concurrent.CompletableFuture;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class OrderEventProducerTest {

    @Mock
    private KafkaTemplate<String, OrderEvent> kafkaTemplate;

    @InjectMocks
    private OrderEventProducer orderEventProducer;

    private OrderEvent mockEvent;

    @BeforeEach
    void setUp() {
        mockEvent = OrderEvent.builder()
                .orderId("ORD-8888")
                .userId(100L)
                .productName("iPhone 16 Pro Max")
                .quantity(1)
                .totalAmount(1499.0)
                .createdAt(LocalDateTime.now())
                .build();
    }

    @Test
    @DisplayName("Test 2: Gửi OrderEvent thành công tới Kafka Topic với JsonSerializer")
    void sendOrderEvent_Success() {
        SendResult<String, OrderEvent> mockResult = mock(SendResult.class, RETURNS_DEEP_STUBS);
        when(kafkaTemplate.send(eq("order-events"), eq("ORD-8888"), eq(mockEvent)))
                .thenReturn(CompletableFuture.completedFuture(mockResult));

        Mono<SendResult<String, OrderEvent>> monoResult = orderEventProducer.sendOrderEvent(mockEvent);

        StepVerifier.create(monoResult)
                .expectNext(mockResult)
                .verifyComplete();

        verify(kafkaTemplate, times(1)).send(eq("order-events"), eq("ORD-8888"), eq(mockEvent));
    }
}
