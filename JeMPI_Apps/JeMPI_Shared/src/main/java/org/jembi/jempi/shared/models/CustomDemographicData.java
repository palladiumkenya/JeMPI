package org.jembi.jempi.shared.models;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record CustomDemographicData(
      String auxId,
      String auxDwhId,
      String patientPkv,
      String siteCode,
      String patientPk,
      String dob,
      String nupi) {

}

