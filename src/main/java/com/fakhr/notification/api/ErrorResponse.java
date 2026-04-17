package com.fakhr.notification.api;

import java.time.Instant;
import java.util.Map;

public record ErrorResponse(
        int status,
        String error,
        Map<String, String> details,
        Instant timestamp
) {
}
