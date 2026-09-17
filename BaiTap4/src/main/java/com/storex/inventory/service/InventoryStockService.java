package com.storex.inventory.service;

import com.storex.inventory.model.OrderCreatedEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

@Service
public class InventoryStockService {

    private static final Logger log = LoggerFactory.getLogger(InventoryStockService.class);

    public void processDeductStock(OrderCreatedEvent event) {
        if (event == null) {
            throw new IllegalArgumentException("Sự kiện OrderCreatedEvent không được null");
        }
        if (event.getProductName() == null || event.getProductName().trim().isEmpty()) {
            throw new IllegalArgumentException("Tên sản phẩm không tồn tại (productName: null)");
        }
        if (event.getQuantity() == null || event.getQuantity() <= 0) {
            throw new IllegalArgumentException("Số lượng sản phẩm không hợp lệ (quantity: null hoặc <= 0)");
        }

        log.info("Trừ kho thành công cho Đơn hàng ID: {}, Sản phẩm: {}, Số lượng: {}",
                event.getOrderId(), event.getProductName(), event.getQuantity());
    }
}
