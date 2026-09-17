package com.storex.producer.controller;

import com.storex.producer.model.dto.OrderRequest;
import com.storex.producer.model.dto.OrderResponse;
import com.storex.producer.service.OrderEventProducerService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

@RestController
@RequestMapping("/api/v1/orders")
public class OrderController {

    private final OrderEventProducerService orderEventProducerService;

    @Autowired
    public OrderController(OrderEventProducerService orderEventProducerService) {
        this.orderEventProducerService = orderEventProducerService;
    }

    @PostMapping
    public Mono<ResponseEntity<OrderResponse>> createOrder(@RequestBody OrderRequest request) {
        return orderEventProducerService.processAndPublishOrder(request)
                .map(orderId -> ResponseEntity.status(HttpStatus.ACCEPTED) // Trả về 202 ACCEPTED ngay lập tức
                        .body(OrderResponse.builder()
                                .orderId(orderId)
                                .status("ACCEPTED")
                                .message("Đơn hàng đã được tiếp nhận và xếp hàng xử lý bất đồng bộ")
                                .createdAt(LocalDateTime.now())
                                .build()));
    }
}
