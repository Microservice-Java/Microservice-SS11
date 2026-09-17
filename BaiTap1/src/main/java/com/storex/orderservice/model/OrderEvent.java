package com.storex.orderservice.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderEvent {
    private String orderId;
    private Long userId;
    private String productName;
    private Integer quantity;
    private Double totalAmount;
    private LocalDateTime createdAt;
}
