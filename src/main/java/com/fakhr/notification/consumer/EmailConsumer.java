package com.fakhr.notification.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fakhr.notification.domain.DeliveryOutcome;
import com.fakhr.notification.domain.NotificationChannel;
import com.fakhr.notification.domain.NotificationLog;
import com.fakhr.notification.domain.NotificationMessage;
import com.fakhr.notification.domain.NotificationStatus;
import com.fakhr.notification.orchestrator.KafkaTopicConfig;
import com.fakhr.notification.repository.NotificationLogRepository;
import io.github.resilience4j.circuitbreaker.CallNotPermittedException;
import io.github.resilience4j.circuitbreaker.CircuitBreaker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.util.Optional;

@Service
public class EmailConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(EmailConsumer.class);

    private final ObjectMapper objectMapper;
    private final CircuitBreaker emailCircuitBreaker;
    private final NotificationLogRepository notificationLogRepository;

    public EmailConsumer(
            ObjectMapper objectMapper,
            @Qualifier("emailCircuitBreaker") CircuitBreaker emailCircuitBreaker,
            NotificationLogRepository notificationLogRepository
    ) {
        this.objectMapper = objectMapper;
        this.emailCircuitBreaker = emailCircuitBreaker;
        this.notificationLogRepository = notificationLogRepository;
    }

    @KafkaListener(topics = KafkaTopicConfig.TOPIC_NOTIFICATIONS_EMAIL)
    public void consume(String payload) {
        NotificationMessage message = deserialize(payload);
        if (message == null) {
            return;
        }

        try {
            emailCircuitBreaker.executeRunnable(() -> LOGGER.info("Sending email to {}", message.recipient()));
            updateNotificationLog(message, NotificationStatus.SENT, DeliveryOutcome.SUCCESS, null);
        } catch (CallNotPermittedException exception) {
            LOGGER.warn("Circuit breaker open for email channel", exception);
            updateNotificationLog(message, NotificationStatus.FAILED, DeliveryOutcome.PROVIDER_ERROR, "Email circuit breaker open");
        } catch (Exception exception) {
            LOGGER.error("Email delivery failed for notificationId={}", message.notificationId(), exception);
            updateNotificationLog(message, NotificationStatus.FAILED, DeliveryOutcome.PROVIDER_ERROR, exception.getMessage());
        }
    }

    private NotificationMessage deserialize(String payload) {
        try {
            return objectMapper.readValue(payload, NotificationMessage.class);
        } catch (JsonProcessingException exception) {
            LOGGER.error("Failed to deserialize email payload", exception);
            return null;
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
                .channel(NotificationChannel.EMAIL)
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
