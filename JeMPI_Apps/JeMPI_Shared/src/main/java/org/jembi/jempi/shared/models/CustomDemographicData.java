package org.jembi.jempi.shared.models;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record CustomDemographicData(
      String auxId,
      String auxDwhId,
      String phoneticGivenName,
      String phoneticFamilyName,
      String gender,
      String dob,
      String nupi) {

}

