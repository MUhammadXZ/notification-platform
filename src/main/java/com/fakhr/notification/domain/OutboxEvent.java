package com.fakhr.notification.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.hibernate.annotations.CreationTimestamp;

import java.time.Instant;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Entity
@Table(name = "outbox_events")
public class OutboxEvent {

    // Primary key for each outbox row awaiting publication.
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    // Domain aggregate identifier, here the notificationId.
    @Column(nullable = false)
    private String aggregateId;

    // Event name describing what happened in the domain.
    @Column(nullable = false)
    private String eventType;

    // Serialized JSON payload for downstream consumers.
    @Column(nullable = false, columnDefinition = "TEXT")
    private String payload;

    // Flag indicating whether this event has been published to Kafka.
    @Builder.Default
    @Column(nullable = false)
    private boolean published = false;

    // Timestamp when the outbox event was created in the database.
    @CreationTimestamp
    @Column(nullable = false, updatable = false)
    private Instant createdAt;

    // Timestamp when publication completed successfully.
    private Instant publishedAt;
}
