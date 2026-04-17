package com.fakhr.notification.api;

public interface NotificationService {

    NotificationResponse submit(SendNotificationRequest request, String clientId);
}
