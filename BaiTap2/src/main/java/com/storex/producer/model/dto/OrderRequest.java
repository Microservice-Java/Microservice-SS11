package com.storex.producer.model.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderRequest {
    private Long userId;
    private String productName;
    private Integer quantity;
    private Double totalAmount;
}
