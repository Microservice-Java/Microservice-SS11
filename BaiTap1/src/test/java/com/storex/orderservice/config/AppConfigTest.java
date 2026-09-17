package com.storex.orderservice.config;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.web.reactive.function.client.WebClient;

import static org.junit.jupiter.api.Assertions.*;

class AppConfigTest {

    @Test
    @DisplayName("Test 1: Khởi tạo WebClient Bean thành công và không null")
    void webClientBean_NotNull() {
        AppConfig config = new AppConfig();
        WebClient.Builder builder = WebClient.builder();
        WebClient webClient = config.webClient(builder);

        assertNotNull(webClient, "WebClient Bean phải được khởi tạo thành công và không null");
    }
}
