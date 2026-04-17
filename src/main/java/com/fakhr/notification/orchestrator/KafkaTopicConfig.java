package com.fakhr.notification.orchestrator;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    public static final String TOPIC_NOTIFICATIONS_INCOMING = "notifications.incoming";
    public static final String TOPIC_NOTIFICATIONS_EMAIL = "notifications.email";
    public static final String TOPIC_NOTIFICATIONS_SMS = "notifications.sms";
    public static final String TOPIC_NOTIFICATIONS_PUSH = "notifications.push";
    public static final String TOPIC_NOTIFICATIONS_RETRY = "notifications.retry";
    public static final String TOPIC_NOTIFICATIONS_DLQ = "notifications.dlq";

    @Bean
    public NewTopic notificationsIncomingTopic() {
        return TopicBuilder.name(TOPIC_NOTIFICATIONS_INCOMING)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic notificationsEmailTopic() {
        return TopicBuilder.name(TOPIC_NOTIFICATIONS_EMAIL)
                .partitions(2)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic notificationsSmsTopic() {
        return TopicBuilder.name(TOPIC_NOTIFICATIONS_SMS)
                .partitions(2)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic notificationsPushTopic() {
        return TopicBuilder.name(TOPIC_NOTIFICATIONS_PUSH)
                .partitions(2)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic notificationsRetryTopic() {
        return TopicBuilder.name(TOPIC_NOTIFICATIONS_RETRY)
                .partitions(1)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic notificationsDlqTopic() {
        return TopicBuilder.name(TOPIC_NOTIFICATIONS_DLQ)
                .partitions(1)
                .replicas(1)
                .build();
    }
}
