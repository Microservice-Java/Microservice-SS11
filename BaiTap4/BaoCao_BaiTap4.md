# BÁO CÁO BÀI TẬP THỰC HÀNH 4: CHỐNG CHỊU LỖI VÀ XỬ LÝ HỘP THƯ CHẾT (DEAD LETTER QUEUE)

---

## 1. PHÂN TÍCH VÀ GIẢI PHÁP TĂNG CƯỜNG TÍNH KHẢ DỤNG (FAULT TOLERANCE)

### 🔴 Lỗi Infinite Loop Blocking Partition khi gặp Message hỏng:
* **Vấn đề**: Tại Inventory-Service, khi Consumer đọc trúng sự kiện lỗi định dạng JSON hoặc thông tin sản phẩm không tồn tại (`productId: null`), hàm `@KafkaListener` văng ngoại lệ `Exception`.
* **Cơ chế**: Do không được commit offset, Consumer rơi vào vòng lặp vô tận (Infinite Loop), liên tục đọc đi đọc lại tin nhắn hỏng đó. Điều này gây nên sự cố **Blocking Partition**, hàng vạn đơn hàng hợp lệ phía sau bị ngưng trệ hoàn toàn.

---

### 🟢 Cách khắc phục triệt để bằng Spring Kafka ErrorHandler & DLQ:

1. **🔴 BUG-05 FIX: Thêm `spring.json.trusted.packages: "*"`**:
   - Spring Kafka mặc định từ chối giải mã JSON nếu class DTO không thuộc danh sách tin cậy (Lỗi: *"The class is not in the trusted packages"*).
   - Thêm `spring.json.trusted.packages: "*"` trong `application.yml` cho phép Consumer tự động deserialization các DTO Object an toàn.

2. **⚙️ Cơ chế Retry 3 lần (cách 2s)**:
   - Cấu hình `DefaultErrorHandler` sử dụng `FixedBackOff(2000L, 2L)` (1 lần thử đầu + 2 lần retry = 3 lượt thử, mỗi lần cách nhau 2 giây).

3. **⚙️ Cơ chế Recovery ném vào DLQ & REQ-02 Log**:
   - Sử dụng `DeadLetterPublishingRecoverer`: Nếu sau 3 lần thử vẫn thất bại, tin nhắn hỏng được tự động đẩy sang Dead Letter Topic `storex-order-events.DLQ`.
   - **REQ-02 FIX**: Ghi nhận log mức ERROR thông báo: `"Đã ném đơn hàng bị lỗi vào DLQ"` kèm nguyên nhân lỗi để phục vụ monitoring.
   - Consumer chính lập tức commit offset tin nhắn hỏng và tiếp tục đọc các tin nhắn tiếp theo mà không bị kẹt.

---

## 2. KẾT QUẢ UNIT TEST (`gradlew test`)

Tất cả các Unit Test đều đã chạy **PASS 100%**:

```text
InventoryOrderConsumerTest > Test 1: Consumer xử lý đơn hàng hợp lệ thành công PASSED
InventoryOrderConsumerTest > Test 2: Consumer nhận dữ liệu hỏng văng Exception (Kích hoạt Retry 3x & DLQ) PASSED

BUILD SUCCESSFUL in 13s
```
