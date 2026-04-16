# Project: Smart Notification Orchestrator

## What this project is
A production-grade, event-driven notification platform built to demonstrate
senior-level backend engineering. Every architectural decision must be
deliberate and explainable in a job interview.

## Stack
- Java 17, Spring Boot, Maven
- Apache Kafka (messaging backbone)
- PostgreSQL (notification log, outbox table, SLA records)
- Redis (deduplication keys, rate limiting token buckets)
- Resilience4j (circuit breakers on all channel consumers)
- Docker Compose (local infrastructure)
- Prometheus + Micrometer + Zipkin (observability)
- JUnit 5 + Mockito (80%+ test coverage target)

## Architecture rules — never break these
- API layer writes to PostgreSQL outbox table and Kafka in one transaction (Transactional Outbox pattern)
- All inter-service communication goes through Kafka topics, never direct HTTP
- Each channel (email, sms, push) is an independent Kafka consumer with its own Resilience4j circuit breaker
- Circuit breaker trips when provider error rate exceeds 15%
- Redis handles deduplication (TTL-based idempotency keys) and rate limiting (token bucket per client)
- Fallback chain order: email → sms → push
- HIGH and NORMAL priority traffic use separate Kafka partitions
- Orchestrator runs as 2 instances minimum — Kafka handles partition rebalancing automatically

## Kafka topics
- notifications.incoming
- notifications.email (partitions: HIGH, NORMAL)
- notifications.sms (partitions: HIGH, NORMAL)
- notifications.push (partitions: HIGH, NORMAL)
- notifications.retry
- notifications.dlq

## Package structure
com.fakhr.notification
  ├── api          (controllers, DTOs, request validation)
  ├── outbox       (Transactional Outbox publisher)
  ├── orchestrator (routing logic, deduplication, rate limiting)
  ├── consumer     (email, sms, push consumers)
  ├── circuitbreaker (Resilience4j config)
  ├── retry        (retry + DLQ logic)
  ├── domain       (entities, enums, Kafka message schema)
  ├── repository   (JPA repositories)
  └── config       (Kafka, Redis, Postgres, Zipkin config)

## Code style
- Clean architecture: business logic never touches framework code directly
- Every public method on a service class must have a unit test
- No magic numbers — use named constants or config properties
- Explain the WHY in comments, not the WHAT
- Each commit should be one logical unit (e.g. "Add Redis deduplication to OrchestratorService")

## Teaching mode
I am learning while building. For every non-trivial implementation:
1. Explain what problem this solves
2. Explain why this approach over alternatives
3. Point out what interviewers typically ask about this pattern