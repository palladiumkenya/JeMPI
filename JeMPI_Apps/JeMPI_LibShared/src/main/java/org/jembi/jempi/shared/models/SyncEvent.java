package org.jembi.jempi.shared.models;

import com.fasterxml.jackson.annotation.JsonInclude;

import java.time.LocalDateTime;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record SyncEvent(
        LocalDateTime createdAt,
        String event) {
}
