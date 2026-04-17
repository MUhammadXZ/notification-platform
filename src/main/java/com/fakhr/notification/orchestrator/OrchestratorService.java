package com.fakhr.notification.orchestrator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fakhr.notification.domain.DeliveryOutcome;
import com.fakhr.notification.domain.NotificationChannel;
import com.fakhr.notification.domain.NotificationLog;
import com.fakhr.notification.domain.NotificationMessage;
import com.fakhr.notification.domain.NotificationStatus;
import com.fakhr.notification.repository.NotificationLogRepository;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.Optional;

@Service
public class OrchestratorService {

    private static final Logger LOGGER = LoggerFactory.getLogger(OrchestratorService.class);
    private static final String PRIORITY_HEADER = "priority";

    private final ObjectMapper objectMapper;
    private final RedisDeduplicationService redisDeduplicationService;
    private final RedisRateLimiter redisRateLimiter;
    private final NotificationLogRepository notificationLogRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public OrchestratorService(
            ObjectMapper objectMapper,
            RedisDeduplicationService redisDeduplicationService,
            RedisRateLimiter redisRateLimiter,
            NotificationLogRepository notificationLogRepository,
            KafkaTemplate<String, String> kafkaTemplate
    ) {
        this.objectMapper = objectMapper;
        this.redisDeduplicationService = redisDeduplicationService;
        this.redisRateLimiter = redisRateLimiter;
        this.notificationLogRepository = notificationLogRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = KafkaTopicConfig.TOPIC_NOTIFICATIONS_INCOMING)
    public void processIncomingNotification(String payload) {
        NotificationMessage message = deserializeMessage(payload);
        if (message == null) {
            return;
        }

        if (redisDeduplicationService.isDuplicate(message.idempotencyKey(), message.clientId())) {
            LOGGER.warn("Duplicate notification detected for notificationId={}", message.notificationId());
            updateNotificationLog(message, NotificationStatus.FAILED, DeliveryOutcome.DEDUPLICATED, "Duplicate idempotency key");
            return;
        }

        if (redisRateLimiter.isRateLimited(message.clientId())) {
            LOGGER.warn("Rate limit exceeded for notificationId={} clientId={}", message.notificationId(), message.clientId());
            updateNotificationLog(message, NotificationStatus.FAILED, DeliveryOutcome.RATE_LIMITED, "Client request rate exceeded");
            return;
        }

        routeToChannelTopic(message);
        updateNotificationLog(message, NotificationStatus.PENDING, null, null);
    }

    private NotificationMessage deserializeMessage(String payload) {
        try {
            return objectMapper.readValue(payload, NotificationMessage.class);
        } catch (JsonProcessingException exception) {
            LOGGER.error("Failed to deserialize incoming notification payload", exception);
            return null;
        }
    }

    private void routeToChannelTopic(NotificationMessage message) {
        String targetTopic = resolveChannelTopic(message.channel());
        String serializedMessage = serializeMessage(message);

        ProducerRecord<String, String> record =
                new ProducerRecord<>(targetTopic, message.notificationId(), serializedMessage);
        record.headers().add(PRIORITY_HEADER, message.priority().name().getBytes(StandardCharsets.UTF_8));

        kafkaTemplate.send(record).whenComplete((result, exception) -> {
            if (exception == null) {
                LOGGER.debug("Routed notificationId={} to topic={}", message.notificationId(), targetTopic);
                return;
            }

            LOGGER.error("Failed to route notificationId={} to topic={}", message.notificationId(), targetTopic, exception);
            updateNotificationLog(
                    message,
                    NotificationStatus.FAILED,
                    DeliveryOutcome.PROVIDER_ERROR,
                    "Kafka routing failure: " + exception.getMessage()
            );
        });
    }

    private String resolveChannelTopic(NotificationChannel channel) {
        return switch (channel) {
            case EMAIL -> KafkaTopicConfig.TOPIC_NOTIFICATIONS_EMAIL;
            case SMS -> KafkaTopicConfig.TOPIC_NOTIFICATIONS_SMS;
            case PUSH -> KafkaTopicConfig.TOPIC_NOTIFICATIONS_PUSH;
        };
    }

    private String serializeMessage(NotificationMessage message) {
        try {
            return objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Failed to serialize notification message for channel routing", exception);
        }
    }

    private void updateNotificationLog(
            NotificationMessage message,
            NotificationStatus status,
            DeliveryOutcome outcome,
            String failureReason
    ) {
        Optional<NotificationLog> existingLog = notificationLogRepository
                .findTopByNotificationIdOrderByCreatedAtDesc(message.notificationId());

        NotificationLog notificationLog = existingLog.orElseGet(() -> NotificationLog.builder()
                .notificationId(message.notificationId())
                .clientId(message.clientId())
                .channel(message.channel())
                .priority(message.priority())
                .recipient(message.recipient())
                .attemptNumber(message.attemptNumber())
                .build());

        notificationLog.setStatus(status);
        notificationLog.setOutcome(outcome);
        notificationLog.setFailureReason(failureReason);
        notificationLogRepository.save(notificationLog);
    }
}
