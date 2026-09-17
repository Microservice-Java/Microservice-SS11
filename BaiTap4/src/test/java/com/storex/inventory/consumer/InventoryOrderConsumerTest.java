package com.storex.inventory.consumer;

import com.storex.inventory.model.OrderCreatedEvent;
import com.storex.inventory.service.InventoryStockService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class InventoryOrderConsumerTest {

    @Mock
    private InventoryStockService inventoryStockService;

    @InjectMocks
    private InventoryOrderConsumer inventoryOrderConsumer;

    private OrderCreatedEvent validEvent;
    private OrderCreatedEvent invalidEvent;

    @BeforeEach
    void setUp() {
        validEvent = OrderCreatedEvent.builder()
                .orderId("ORD-5555")
                .userId(101L)
                .productName("MacBook Air M3")
                .quantity(1)
                .totalAmount(1299.0)
                .createdAt(LocalDateTime.now())
                .build();

        invalidEvent = OrderCreatedEvent.builder()
                .orderId("ORD-9999")
                .userId(102L)
                .productName(null) // Lỗi: productId/productName is null
                .quantity(0)
                .totalAmount(0.0)
                .createdAt(LocalDateTime.now())
                .build();
    }

    @Test
    @DisplayName("Test 1: Consumer xử lý đơn hàng hợp lệ thành công")
    void consumeOrderEvent_Success() {
        doNothing().when(inventoryStockService).processDeductStock(validEvent);

        inventoryOrderConsumer.consumeOrderEvent(validEvent);

        verify(inventoryStockService, times(1)).processDeductStock(validEvent);
    }

    @Test
    @DisplayName("Test 2: Consumer nhận dữ liệu hỏng văng Exception (Kích hoạt Retry 3x & DLQ)")
    void consumeOrderEvent_InvalidData_ThrowsException() {
        doThrow(new IllegalArgumentException("Tên sản phẩm không tồn tại (productName: null)"))
                .when(inventoryStockService).processDeductStock(invalidEvent);

        IllegalArgumentException thrown = assertThrows(
                IllegalArgumentException.class,
                () -> inventoryOrderConsumer.consumeOrderEvent(invalidEvent)
        );

        assertTrue(thrown.getMessage().contains("Tên sản phẩm không tồn tại"));
        verify(inventoryStockService, times(1)).processDeductStock(invalidEvent);
    }
}
