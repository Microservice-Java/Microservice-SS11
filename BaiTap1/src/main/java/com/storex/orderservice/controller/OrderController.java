package com.storex.orderservice.controller;

import com.storex.orderservice.model.OrderEvent;
import com.storex.orderservice.producer.OrderEventProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

import java.time.LocalDateTime;

@RestController
@RequestMapping("/api/orders")
public class OrderController {

    private final OrderEventProducer orderEventProducer;

    @Autowired
    public OrderController(OrderEventProducer orderEventProducer) {
        this.orderEventProducer = orderEventProducer;
    }

    @GetMapping("/health")
    public Mono<String> healthCheck() {
        return Mono.just("Order Service (Reactive WebFlux Netty) is running on port 8080");
    }

    @PostMapping
    public Mono<ResponseEntity<OrderEvent>> createOrder(@RequestBody OrderEvent orderEvent) {
        if (orderEvent.getCreatedAt() == null) {
            orderEvent.setCreatedAt(LocalDateTime.now());
        }

        return orderEventProducer.sendOrderEvent(orderEvent)
                .map(result -> ResponseEntity.ok(orderEvent))
                .onErrorReturn(ResponseEntity.internalServerError().build());
    }
}
