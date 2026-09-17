package com.storex.orderservice.config;

import io.netty.channel.ChannelOption;
import io.netty.handler.timeout.ReadTimeoutHandler;
import io.netty.handler.timeout.WriteTimeoutHandler;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.client.reactive.ReactorClientHttpConnector;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.netty.http.client.HttpClient;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

@Configuration
public class AppConfig {

    /**
     * Khởi tạo WebClient Bean dùng chung cho toàn dự án.
     * Cấu hình Timeout ngắt kết nối tự động sau 5 giây nếu gọi sang service khác không nhận được phản hồi.
     */
    @Bean
    public WebClient webClient(WebClient.Builder builder) {
        HttpClient httpClient = HttpClient.create()
                .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 5000) // Connect Timeout 5s
                .responseTimeout(Duration.ofSeconds(5))            // Response Timeout 5s
                .doOnConnected(conn -> conn
                        .addHandlerLast(new ReadTimeoutHandler(5, TimeUnit.SECONDS))  // Read Timeout 5s
                        .addHandlerLast(new WriteTimeoutHandler(5, TimeUnit.SECONDS))); // Write Timeout 5s

        return builder
                .clientConnector(new ReactorClientHttpConnector(httpClient))
                .build();
    }
}
