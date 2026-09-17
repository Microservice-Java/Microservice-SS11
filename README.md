# SESSION 11 - REACTIVE MICROSERVICES & EVENT-DRIVEN ARCHITECTURE WITH KAFKA

Hệ thống **StoreX E-Commerce** - Session 11: Nâng cấp Order-Service lên kiến trúc Non-blocking Reactive (Spring WebFlux + Netty) và kết nối Apache Kafka Event Broker.

---

## 📁 Cấu trúc Thư mục Dự án

```text
SS11/
├── BaiTap1/
│   ├── build.gradle
│   ├── settings.gradle
│   ├── BaoCao_BaiTap1.md
│   └── src/
│       ├── main/java/com/storex/orderservice/
│       └── test/java/com/storex/orderservice/
├── BaiTap2/
│   ├── build.gradle
│   ├── settings.gradle
│   ├── BaoCao_BaiTap2.md
│   └── src/
│       ├── main/java/com/storex/producer/
│       └── test/java/com/storex/producer/
├── BaiTap4/
│   ├── build.gradle
│   ├── settings.gradle
│   ├── BaoCao_BaiTap4.md
│   └── src/
│       ├── main/java/com/storex/inventory/
│       │   ├── InventoryDlqApplication.java
│       │   ├── config/KafkaErrorHandlerConfig.java
│       │   ├── consumer/InventoryDlqConsumer.java
│       │   ├── consumer/InventoryOrderConsumer.java
│       │   ├── model/OrderCreatedEvent.java
│       │   └── service/InventoryStockService.java
│       └── test/java/com/storex/inventory/
│           └── consumer/InventoryOrderConsumerTest.java
└── postman/
    ├── SS11_BaiTap1_Collection.json
    ├── SS11_BaiTap2_Collection.json
    └── SS11_BaiTap4_Collection.json
```

---

## 📝 Tóm tắt các Bài tập

### **Bài tập 1**: Khởi tạo Nền tảng Non-blocking và Kết nối Kafka Cluster
- Dùng `spring-boot-starter-webflux` chạy Netty Server (Port 8080), loại bỏ hoàn toàn Tomcat (BUG-01).
- Cấu hình WebClient Bean với timeout 5 giây.
- Kết nối Kafka Broker tại `localhost:9092` với `JsonSerializer` cho Value (BUG-02).

### **Bài tập 2**: Xây dựng API Đặt hàng Bất đồng bộ (Event Producer)
- Xây dựng Reactive API `POST /api/v1/orders` trả về HTTP Status 202 Accepted.
- Khởi tạo và phát sự kiện `order.created` vào Kafka Topic `storex-order-events` (5 Partitions).
- **BUG-03 FIX**: Truyền `orderId` làm khóa (Key) phân tuyến trong `kafkaTemplate.send("storex-order-events", orderId, event)` để đảm bảo tất cả sự kiện của cùng 1 đơn hàng luôn rơi vào cùng 1 Partition (Ordering Guarantee).

### **Bài tập 4**: Chống chịu Lỗi và Xử lý Hộp thư chết (Dead Letter Queue - DLQ)
- Giải quyết sự cố Consumer bị kẹt khi nhận message JSON hỏng hoặc `productId: null`.
- **BUG-05 FIX**: Bổ sung `spring.json.trusted.packages: "*"` cho phép Kafka tự động giải mã JSON Object.
- Cấu hình `DefaultErrorHandler` thử lại (Retry) 3 lần cách nhau 2 giây (`FixedBackOff(2000L, 2L)`).
- **REQ-02 FIX**: Tích hợp `DeadLetterPublishingRecoverer` ném tin nhắn hỏng vào topic `storex-order-events.DLQ` và in log mức ERROR: `"Đã ném đơn hàng bị lỗi vào DLQ"`.

---

## 🧪 Kết quả Execution Unit Test

```text
[BaiTap1] AppConfigTest & OrderEventProducerTest     -> PASSED (BUILD SUCCESSFUL)
[BaiTap2] OrderEventProducerServiceTest (BUG-03 Test) -> PASSED (BUILD SUCCESSFUL)
[BaiTap4] InventoryOrderConsumerTest (BUG-05 & REQ-02)-> PASSED (BUILD SUCCESSFUL)
```
