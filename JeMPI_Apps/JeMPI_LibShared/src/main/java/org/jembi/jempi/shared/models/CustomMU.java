package org.jembi.jempi.shared.models;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record CustomMU(String tag,
                       Probability givenName,
                       Probability familyName,
                       Probability gender,
                       Probability dob,
                       Probability city,
                       Probability phoneNumber,
                       Probability nationalId) {



   public record Probability(float m, float u) {
   }

}
