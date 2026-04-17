package com.fakhr.notification.api;

import com.fakhr.notification.domain.NotificationChannel;
import com.fakhr.notification.domain.NotificationPriority;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

public record SendNotificationRequest(
        @NotNull(message = "channel is required")
        NotificationChannel channel,
        NotificationPriority priority,
        @NotBlank(message = "recipient is required")
        String recipient,
        String subject,
        @NotBlank(message = "body is required")
        String body,
        @NotBlank(message = "clientId is required")
        String clientId,
        @NotBlank(message = "idempotencyKey is required")
        String idempotencyKey
) {
    public SendNotificationRequest {
        if (priority == null) {
            priority = NotificationPriority.NORMAL;
        }
    }
}
