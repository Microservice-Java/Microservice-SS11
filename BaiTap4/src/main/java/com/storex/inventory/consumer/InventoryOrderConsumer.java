package com.storex.inventory.consumer;

import com.storex.inventory.model.OrderCreatedEvent;
import com.storex.inventory.service.InventoryStockService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class InventoryOrderConsumer {

    private static final Logger log = LoggerFactory.getLogger(InventoryOrderConsumer.class);
    private final InventoryStockService inventoryStockService;

    @Autowired
    public InventoryOrderConsumer(InventoryStockService inventoryStockService) {
        this.inventoryStockService = inventoryStockService;
    }

    @KafkaListener(topics = "storex-order-events", groupId = "inventory-dlq-group")
    public void consumeOrderEvent(OrderCreatedEvent event) {
        log.info("Nhận order-event từ topic 'storex-order-events': OrderId={}, ProductName={}, Quantity={}",
                event.getOrderId(), event.getProductName(), event.getQuantity());

        inventoryStockService.processDeductStock(event);
    }
}
