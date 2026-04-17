package com.fakhr.notification.api;

import com.fakhr.notification.domain.NotificationStatus;

import java.time.Instant;

public record NotificationResponse(
        String notificationId,
        NotificationStatus status,
        String message,
        Instant acceptedAt
) {
}
