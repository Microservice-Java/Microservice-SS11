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
│       │   ├── OrderProducerApplication.java
│       │   ├── config/KafkaTopicConfig.java
│       │   ├── controller/OrderController.java
│       │   ├── model/dto/OrderRequest.java
│       │   ├── model/dto/OrderResponse.java
│       │   ├── model/event/OrderCreatedEvent.java
│       │   └── service/OrderEventProducerService.java
│       └── test/java/com/storex/producer/
│           └── service/OrderEventProducerServiceTest.java
└── postman/
    ├── SS11_BaiTap1_Collection.json
    └── SS11_BaiTap2_Collection.json
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

---

## 🧪 Kết quả Execution Unit Test

```text
[BaiTap1] AppConfigTest & OrderEventProducerTest     -> PASSED (BUILD SUCCESSFUL)
[BaiTap2] OrderEventProducerServiceTest (BUG-03 Test) -> PASSED (BUILD SUCCESSFUL)
```
