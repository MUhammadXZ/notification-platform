package com.fakhr.notification.outbox;

import com.fakhr.notification.domain.OutboxEvent;
import com.fakhr.notification.repository.OutboxEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Service
public class OutboxPublisher {

    private static final Logger LOGGER = LoggerFactory.getLogger(OutboxPublisher.class);
    private static final String INCOMING_TOPIC = "notifications.incoming";
    private static final long PUBLISH_FIXED_DELAY_MS = 5000L;
    private static final long KAFKA_SEND_TIMEOUT_SECONDS = 10L;

    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public OutboxPublisher(OutboxEventRepository outboxEventRepository, KafkaTemplate<String, String> kafkaTemplate) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelay = PUBLISH_FIXED_DELAY_MS)
    public void publishPendingEvents() {
        List<OutboxEvent> pendingEvents = outboxEventRepository.findByPublishedFalseOrderByCreatedAtAsc();

        for (OutboxEvent event : pendingEvents) {
            try {
                kafkaTemplate.send(INCOMING_TOPIC, event.getAggregateId(), event.getPayload())
                        .get(KAFKA_SEND_TIMEOUT_SECONDS, TimeUnit.SECONDS);

                event.setPublished(true);
                event.setPublishedAt(Instant.now());
                outboxEventRepository.save(event);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                LOGGER.error(
                        "Publish interrupted for outbox event id={} and aggregateId={}",
                        event.getId(),
                        event.getAggregateId(),
                        exception
                );
            } catch (Exception exception) {
                LOGGER.error(
                        "Failed to publish outbox event with id={} and aggregateId={}",
                        event.getId(),
                        event.getAggregateId(),
                        exception
                );
            }
        }
    }
}
