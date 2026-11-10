package com.rutaexpress.kafkaadmin;

import java.util.concurrent.TimeUnit;
import org.apache.kafka.clients.admin.Admin;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaAdmin;

@SpringBootApplication
public class KafkaAdminApplication {
    public static void main(String[] args) {
        SpringApplication.run(KafkaAdminApplication.class, args);
    }

    @Bean(destroyMethod = "close")
    Admin topicAdminClient(KafkaAdmin kafkaAdmin) {
        return Admin.create(kafkaAdmin.getConfigurationProperties());
    }

    @Bean
    HealthIndicator kafkaHealthIndicator(Admin admin) {
        return () -> {
            try {
                var brokers = admin.describeCluster().nodes().get(4, TimeUnit.SECONDS);
                return brokers.isEmpty() ? Health.down().build() : Health.up().build();
            } catch (InterruptedException ex) {
                Thread.currentThread().interrupt();
                return Health.down().build();
            } catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException ex) {
                return Health.down().build();
            }
        };
    }
}
