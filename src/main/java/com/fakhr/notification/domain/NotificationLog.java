package com.fakhr.notification.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import java.time.Instant;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "notification_logs")
public class NotificationLog {

    // Primary key for persisted notification log rows.
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    // Business notification identifier shared with Kafka messages.
    @Column(nullable = false)
    private String notificationId;

    // Client identifier used for tenant-level tracking.
    @Column(nullable = false)
    private String clientId;

    // Channel selected for this logged delivery attempt.
    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private NotificationChannel channel;

    // Priority associated with this notification.
    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private NotificationPriority priority;

    // Destination address used by the selected channel.
    @Column(nullable = false)
    private String recipient;

    // Lifecycle status of the notification attempt.
    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private NotificationStatus status;

    // Delivery outcome category for sent/failed states.
    @Enumerated(EnumType.STRING)
    private DeliveryOutcome outcome;

    // Provider or platform error reason when delivery fails.
    private String failureReason;

    // Timestamp when this log record is first inserted.
    @CreationTimestamp
    @Column(nullable = false, updatable = false)
    private Instant createdAt;

    // Timestamp updated automatically on each row update.
    @UpdateTimestamp
    @Column(nullable = false)
    private Instant updatedAt;

    // Attempt sequence number for retries and fallback tracking.
    @Column(nullable = false)
    private int attemptNumber;
}
