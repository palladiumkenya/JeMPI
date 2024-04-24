package org.jembi.jempi.shared.models;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record CustomMU(String tag,
              Probability dummy) {

   public static final Boolean SEND_INTERACTIONS_TO_EM = false;


   public record Probability(float m, float u) {
   }

}
