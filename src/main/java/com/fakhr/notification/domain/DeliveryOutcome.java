package com.fakhr.notification.domain;

public enum DeliveryOutcome {
    SUCCESS,
    PROVIDER_ERROR,
    RATE_LIMITED,
    DEDUPLICATED,
    FALLBACK_TRIGGERED
}
