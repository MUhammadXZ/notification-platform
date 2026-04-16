package com.fakhr.notification.domain;

import java.time.Instant;
import java.util.UUID;

public record NotificationMessage(
        // Unique notification identifier propagated across all stages.
        String notificationId,
        // Tenant or client identifier used for routing, deduplication, and rate limiting.
        String clientId,
        // Target delivery channel for this attempt.
        NotificationChannel channel,
        // Priority used for partitioning and scheduling.
        NotificationPriority priority,
        // Destination address such as email, phone number, or push token.
        String recipient,
        // Optional subject line used by email channel.
        String subject,
        // Message content body delivered to the recipient.
        String body,
        // Idempotency key used to prevent duplicate sends.
        String idempotencyKey,
        // Timestamp when the notification request was created.
        Instant createdAt,
        // Delivery attempt number starting at 1.
        int attemptNumber
) {
    public static NotificationMessage create(
            String clientId,
            NotificationChannel channel,
            NotificationPriority priority,
            String recipient,
            String subject,
            String body,
            String idempotencyKey
    ) {
        return new NotificationMessage(
                UUID.randomUUID().toString(),
                clientId,
                channel,
                priority,
                recipient,
                subject,
                body,
                idempotencyKey,
                Instant.now(),
                1
        );
    }
}
