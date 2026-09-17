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
│       │   ├── OrderServiceApplication.java
│       │   ├── config/AppConfig.java
│       │   ├── controller/OrderController.java
│       │   ├── model/OrderEvent.java
│       │   └── producer/OrderEventProducer.java
│       ├── main/resources/
│       │   └── application.yml
│       └── test/java/com/storex/orderservice/
│           ├── config/AppConfigTest.java
│           └── producer/OrderEventProducerTest.java
└── postman/
    └── SS11_BaiTap1_Collection.json
```

---

## 🛠 Điểm nổi bật trong Bài tập 1

1. **Chuyển đổi sang Netty Server (Loại bỏ Tomcat - BUG-01)**:
   - Dùng `spring-boot-starter-webflux` thay thế `spring-boot-starter-web`.
   - Khởi chạy ứng dụng bằng máy chủ Embedded Netty trên port `8080`.

2. **WebClient Bean với Timeout 5 Giây**:
   - Cấu hình Connect Timeout 5s, Response Timeout 5s, Read/Write Timeout 5s qua Netty `HttpClient`.

3. **Kafka Broker & Value JsonSerializer (BUG-02)**:
   - Kết nối Kafka Cluster tại `localhost:9092`.
   - Cấu hình `JsonSerializer` truyền tải đối tượng `OrderEvent` dạng JSON qua Kafka topic `order-events`.

---

## 🧪 Kết quả Unit Test

```text
[BaiTap1] AppConfigTest & OrderEventProducerTest -> All Tests PASSED (BUILD SUCCESSFUL)
```
