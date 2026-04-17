package com.fakhr.notification.outbox;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fakhr.notification.api.NotificationResponse;
import com.fakhr.notification.api.SendNotificationRequest;
import com.fakhr.notification.domain.NotificationLog;
import com.fakhr.notification.domain.NotificationMessage;
import com.fakhr.notification.domain.NotificationStatus;
import com.fakhr.notification.domain.OutboxEvent;
import com.fakhr.notification.repository.NotificationLogRepository;
import com.fakhr.notification.repository.OutboxEventRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.Instant;

@Service
@Transactional
public class OutboxService {

    private static final String NOTIFICATION_REQUESTED_EVENT = "NOTIFICATION_REQUESTED";

    private final NotificationLogRepository notificationLogRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    public OutboxService(
            NotificationLogRepository notificationLogRepository,
            OutboxEventRepository outboxEventRepository,
            ObjectMapper objectMapper
    ) {
        this.notificationLogRepository = notificationLogRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    public NotificationResponse publishNotification(SendNotificationRequest request, String clientId) {
        NotificationMessage message = NotificationMessage.create(
                clientId,
                request.channel(),
                request.priority(),
                request.recipient(),
                request.subject(),
                request.body(),
                request.idempotencyKey()
        );

        NotificationLog notificationLog = NotificationLog.builder()
                .notificationId(message.notificationId())
                .clientId(clientId)
                .channel(message.channel())
                .priority(message.priority())
                .recipient(message.recipient())
                .status(NotificationStatus.PENDING)
                .attemptNumber(message.attemptNumber())
                .build();
        notificationLogRepository.save(notificationLog);

        String payload = serializeMessage(message);

        OutboxEvent outboxEvent = OutboxEvent.builder()
                .aggregateId(message.notificationId())
                .eventType(NOTIFICATION_REQUESTED_EVENT)
                .payload(payload)
                .published(false)
                .build();
        outboxEventRepository.save(outboxEvent);

        return new NotificationResponse(
                message.notificationId(),
                NotificationStatus.PENDING,
                "Notification accepted",
                Instant.now()
        );
    }

    private String serializeMessage(NotificationMessage message) {
        try {
            return objectMapper.writeValueAsString(message);
        } catch (JsonProcessingException exception) {
            throw new IllegalStateException("Failed to serialize notification message", exception);
        }
    }
}
