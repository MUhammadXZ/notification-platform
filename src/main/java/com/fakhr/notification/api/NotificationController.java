package com.fakhr.notification.api;

import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@Validated
@RestController
@RequestMapping("/api/v1/notifications")
public class NotificationController {

    private final NotificationService notificationService;

    public NotificationController(NotificationService notificationService) {
        this.notificationService = notificationService;
    }

    @PostMapping
    public ResponseEntity<NotificationResponse> submitNotification(
            @Valid @RequestBody SendNotificationRequest request,
            @RequestHeader("X-Client-ID") String clientId
    ) {
        if (!clientId.equals(request.clientId())) {
            throw new IllegalArgumentException("X-Client-ID header must match request clientId");
        }

        NotificationResponse response = notificationService.submit(request, clientId);
        return ResponseEntity.status(HttpStatus.ACCEPTED).body(response);
    }
}
