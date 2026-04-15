# Notification Platform

A production-grade Spring Boot notification platform designed around event-driven architecture.  
It is built to support reliable, scalable notification delivery using Kafka, PostgreSQL, Redis, and observability tooling.  
This repository currently focuses on infrastructure and configuration foundations before business logic implementation.

## Prerequisites

- Java 17
- Maven 3.9+
- Docker and Docker Compose

## Run locally

1. Copy environment template:
   - PowerShell: `Copy-Item .env.example .env`
   - Bash: `cp .env.example .env`
2. Open `.env` and set real values for credentials and connection variables.
3. Start infrastructure:
   - `docker compose up -d`
4. Start the Spring Boot app:
   - `./mvnw spring-boot:run`
   - On Windows PowerShell: `./mvnw.cmd spring-boot:run`
