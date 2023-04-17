package org.jembi.jempi.libmpi.dgraph;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jembi.jempi.shared.models.PatientRecordWithScore;
import org.jembi.jempi.shared.models.CustomDemographicData;
import org.jembi.jempi.shared.models.PatientRecord;

@JsonInclude(JsonInclude.Include.NON_NULL)
record CustomDgraphPatientRecord(
      @JsonProperty("uid") String patientId,
      @JsonProperty("PatientRecord.source_id") DgraphSourceId sourceId,
      @JsonProperty("PatientRecord.aux_id") String auxId,
      @JsonProperty("PatientRecord.aux_dwh_id") String auxDwhId,
      @JsonProperty("PatientRecord.patient_pkv") String patientPkv,
      @JsonProperty("PatientRecord.site_code") String siteCode,
      @JsonProperty("PatientRecord.patient_pk") String patientPk,
      @JsonProperty("PatientRecord.dob") String dob,
      @JsonProperty("PatientRecord.nupi") String nupi,
      @JsonProperty("GoldenRecord.patients|score") Float score) {
   CustomDgraphPatientRecord(
         final PatientRecord patientRecord,
         final Float score) {
      this(patientRecord.patientId(),
           new DgraphSourceId(patientRecord.sourceId()),
           patientRecord.demographicData().auxId(),
           patientRecord.demographicData().auxDwhId(),
           patientRecord.demographicData().patientPkv(),
           patientRecord.demographicData().siteCode(),
           patientRecord.demographicData().patientPk(),
           patientRecord.demographicData().dob(),
           patientRecord.demographicData().nupi(),
           score);
   }

   PatientRecord toPatientRecord() {
      return new PatientRecord(this.patientId(),
                               this.sourceId() != null
                                     ? this.sourceId().toSourceId()
                                     : null,
                               new CustomDemographicData(this.auxId(),
                                                         this.auxDwhId(),
                                                         this.patientPkv(),
                                                         this.siteCode(),
                                                         this.patientPk(),
                                                         this.dob(),
                                                         this.nupi()));
   }

   PatientRecordWithScore toPatientRecordWithScore() {
      return new PatientRecordWithScore(toPatientRecord(), this.score());
   }

}
