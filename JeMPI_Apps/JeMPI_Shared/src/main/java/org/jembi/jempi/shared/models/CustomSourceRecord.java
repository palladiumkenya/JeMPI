package org.jembi.jempi.shared.models;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record CustomSourceRecord(
      // System Trace Audit Number
      String stan,
      SourceId sourceId,
      String auxId,
      String auxDwhId,
      String pkv,
      String siteCode,
      String patientPk,
      String nupi) {
}

