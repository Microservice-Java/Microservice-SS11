# BÁO CÁO BÀI TẬP THỰC HÀNH 2: XÂY DỰNG API ĐẶT HÀNG BẤT ĐỒNG BỘ (EVENT PRODUCER)

---

## 1. PHÂN TÍCH VÀ XỬ LÝ BUG-03: ĐẢM BẢO TÍNH TOÀN VẸN THỨ TỰ VỚI KAFKA PARTITION

### 🔴 BUG-03: Sự cố mất trật tự sự kiện (Out-of-order Events) trong Topic có 5 Partitions
* **Vấn đề**: Topic `storex-order-events` được cấu hình phân chia thành **5 Partitions**. Khi gọi `kafkaTemplate.send(topic, event)` mà **không chỉ định Key (Key = null)**:
  - Kafka mặc định sử dụng thuật toán **Sticky / Round-Robin Partitioning** để phân bổ các message luân phiên đều vào cả 5 partitions.
  - Hậu quả: Sự kiện khởi tạo đơn hàng (`order.created`), sự kiện cập nhật số lượng (`order.updated`), và sự kiện hủy đơn (`order.cancelled`) của CÙNG MỘT ĐƠN HÀNG (`orderId`) sẽ bị rơi vào các Partition khác nhau.
  - Do Kafka chỉ đảm bảo thứ tự message **TRONG CÙNG 1 PARTITION**, các Consumer đọc từ các partition khác nhau có thể xử lý sự kiện `order.cancelled` TRƯỚC KHI xử lý `order.created`, gây sai lệch dữ liệu kho nghiêm trọng.

---

### 🟢 Cách xử lý triệt để BUG-03:
- **Nguyên lý Partition Key Hashing**: Kafka phân tuyến record theo công thức `murmur2(Key) % numPartitions`. Tất cả record có cùng 1 Key chắc chắn 100% luôn được gửi vào **CÙNG MỘT PARTITION**.
- **Giải pháp**: Bắt buộc truyền `orderId` làm khóa (Key) phân tuyến khi gọi lệnh `send()`:

```java
// BUG-03 FIX: BẮT BUỘC TRUYỀN orderId LÀM KHÓA (KEY) PHÂN TUYẾN
CompletableFuture<SendResult<String, OrderCreatedEvent>> future =
        kafkaTemplate.send("storex-order-events", orderId, event);
```

---

## 2. THIẾT KẾ API ĐẶT HÀNG BẤT ĐỒNG BỘ (HTTP 202 ACCEPTED)

- **Endpoint**: `POST /api/v1/orders`
- **Mô hình Reactive Non-blocking**: Sử dụng `Mono<ResponseEntity<OrderResponse>>` trong Spring WebFlux.
- **Phản hồi tức thì**: Thay vì bắt người dùng chờ trừ kho/trừ tiền đồng bộ, API khởi tạo `orderId`, phát sự kiện `order.created` vào Kafka và trả về ngay lập tức **HTTP Status 202 Accepted**:

```json
{
  "orderId": "ORD-E39A1C4B",
  "status": "ACCEPTED",
  "message": "Đơn hàng đã được tiếp nhận và xếp hàng xử lý bất đồng bộ",
  "createdAt": "2026-09-17T08:01:15"
}
```

---

## 3. KẾT QUẢ UNIT TEST (`gradlew test`)

Tất cả các Unit Test đều đã chạy **PASS 100%**:

```text
OrderEventProducerServiceTest > Test BUG-03: Kiểm tra orderId được truyền làm Partition Key trong kafkaTemplate.send() PASSED

BUILD SUCCESSFUL in 20s
```
