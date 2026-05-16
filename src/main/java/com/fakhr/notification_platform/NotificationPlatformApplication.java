package com.fakhr.notification_platform;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.persistence.autoconfigure.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication(scanBasePackages = "com.fakhr.notification")
@EnableJpaRepositories(basePackages = "com.fakhr.notification.repository")
@EntityScan(basePackages = "com.fakhr.notification.domain")
@EnableScheduling
public class NotificationPlatformApplication {

	public static void main(String[] args) {
		SpringApplication.run(NotificationPlatformApplication.class, args);
	}

}
