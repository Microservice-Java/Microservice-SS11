package com.storex.producer.model.event;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderCreatedEvent {
    private String orderId;
    private Long userId;
    private String productName;
    private Integer quantity;
    private Double totalAmount;
    private String status;
    private String eventType;
    private LocalDateTime createdAt;
}
