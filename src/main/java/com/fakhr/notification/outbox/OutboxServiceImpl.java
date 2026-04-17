package com.fakhr.notification.outbox;

import com.fakhr.notification.api.NotificationResponse;
import com.fakhr.notification.api.NotificationService;
import com.fakhr.notification.api.SendNotificationRequest;
import org.springframework.stereotype.Service;

@Service
public class OutboxServiceImpl implements NotificationService {

    private final OutboxService outboxService;

    public OutboxServiceImpl(OutboxService outboxService) {
        this.outboxService = outboxService;
    }

    @Override
    public NotificationResponse submit(SendNotificationRequest request, String clientId) {
        return outboxService.publishNotification(request, clientId);
    }
}
