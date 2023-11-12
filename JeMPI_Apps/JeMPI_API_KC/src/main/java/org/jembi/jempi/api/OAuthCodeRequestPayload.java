package org.jembi.jempi.api;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

@JsonInclude(JsonInclude.Include.NON_NULL)
record OAuthCodeRequestPayload(
      @JsonProperty("code") String code,
      @JsonProperty("state") String state,
      @JsonProperty("session_state") String sessionId) {
}
