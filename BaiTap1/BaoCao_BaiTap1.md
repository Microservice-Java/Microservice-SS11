# BÁO CÁO BÀI TẬP THỰC HÀNH 1: KHỞI TẠO NỀN TẢNG NON-BLOCKING VÀ KẾT NỐI KAFKA CLUSTER

---

## 1. PHÂN TÍCH VÀ XỬ LÝ CÁC LỖI VÀ ĐIỀU KIỆN BIÊN (EDGE CASES)

### 🔴 BUG-01: Loại bỏ máy chủ Tomcat - Khởi chạy ứng dụng bằng Netty WebServer
* **Vấn đề**: Khi thư viện `spring-boot-starter-web` tồn tại trong file cấu hình build, Spring Boot mặc định sử dụng máy chủ Servlet đồng bộ Tomcat (Servlet Container blocking, Thread-per-request model). Trong các dịp Flash Sale cao điểm với 10.000 request/s, Servlet Thread Pool bị cạn kiệt lập tức.
* **Cách xử lý**:
  - Loại bỏ hoàn toàn phụ thuộc `org.springframework.boot:spring-boot-starter-web`.
  - Bổ sung duy nhất phụ thuộc `org.springframework.boot:spring-boot-starter-webflux` trong `build.gradle`.
  - Cấu hình `spring.main.web-application-type: reactive` trong `application.yml`.
* **Kết quả**: Ứng dụng khởi động trên cổng `8080` sử dụng **Embedded Netty WebServer** (Event Loop Non-blocking Reactive Model), đáp ứng khả năng chịu tải hàng chục ngàn kết nối đồng thời.

---

### 🔴 BUG-02: Cấu hình Serializer cho Kafka Value - Bắt buộc dùng `JsonSerializer`
* **Vấn đề**: Nếu sử dụng `StringSerializer` cho Value, việc truyền tải dữ liệu cấu trúc phức tạp như `OrderEvent` (gồm `orderId`, `userId`, `productName`, `quantity`, `totalAmount`, `createdAt`) sẽ bị hỏng hoặc yêu cầu phải ép kiểu chuỗi thủ công.
* **Cách xử lý**:
  - Cấu hình Producer Value Serializer chính xác trong `application.yml`:
    `spring.kafka.producer.value-serializer: org.springframework.kafka.support.serializer.JsonSerializer`
  - Cấu hình Consumer Value Deserializer với `ErrorHandlingDeserializer` bóc tách `JsonDeserializer` và mở `spring.json.trusted.packages: "*"`.
* **Kết quả**: Dữ liệu DTO Java Object được tự động mã hóa (serialize) thành chuỗi JSON chuẩn khi gửi sang Kafka Cluster tại `localhost:9092`.

---

## 2. CẤU HÌNH WEBCLIENT BEAN DÙNG CHUNG VỚI TIMEOUT 5 GIÂY

Trong `AppConfig.java`, WebClient Bean được tạo với Netty `HttpClient` cấu hình timeout tự động ngắt kết nối sau 5 giây:

```java
@Bean
public WebClient webClient(WebClient.Builder builder) {
    HttpClient httpClient = HttpClient.create()
            .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000)
            .responseTimeout(Duration.ofSeconds(5))
            .doOnConnected(conn -> conn
                    .addHandlerLast(new ReadTimeoutHandler(5, TimeUnit.SECONDS))
                    .addHandlerLast(new WriteTimeoutHandler(5, TimeUnit.SECONDS)));

    return builder
            .clientConnector(new ReactorClientHttpConnector(httpClient))
            .build();
}
```

---

## 3. KẾT QUẢ UNIT TEST (`gradlew test`)

Tất cả các Unit Test đều đã chạy **PASS 100%**:

```text
AppConfigTest          > Test 1: Khởi tạo WebClient Bean thành công PASSED
OrderEventProducerTest > Test 2: Gửi OrderEvent thành công tới Kafka Topic với JsonSerializer PASSED

BUILD SUCCESSFUL in 18s
```
