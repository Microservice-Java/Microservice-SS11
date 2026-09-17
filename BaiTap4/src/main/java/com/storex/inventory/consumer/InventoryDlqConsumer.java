package com.storex.inventory.consumer;

import com.storex.inventory.model.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
public class InventoryDlqConsumer {

    private static final Logger log = LoggerFactory.getLogger(InventoryDlqConsumer.class);

    @KafkaListener(topics = "storex-order-events.DLQ", groupId = "inventory-dlq-monitor-group")
    public void consumeDlqEvent(OrderCreatedEvent failedEvent) {
        log.error("[DEAD LETTER QUEUE MONITOR] Đã ném đơn hàng bị lỗi vào DLQ sau 3 lần retry: OrderId={}, ProductName={}, Quantity={}",
                failedEvent.getOrderId(), failedEvent.getProductName(), failedEvent.getQuantity());
    }
}
