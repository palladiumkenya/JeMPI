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
      @JsonProperty("PatientRecord.phonetic_given_name") String phoneticGivenName,
      @JsonProperty("PatientRecord.phonetic_family_name") String phoneticFamilyName,
      @JsonProperty("PatientRecord.gender") String gender,
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
           patientRecord.demographicData().phoneticGivenName(),
           patientRecord.demographicData().phoneticFamilyName(),
           patientRecord.demographicData().gender(),
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
                                                         this.phoneticGivenName(),
                                                         this.phoneticFamilyName(),
                                                         this.gender(),
                                                         this.dob(),
                                                         this.nupi()));
   }

   PatientRecordWithScore toPatientRecordWithScore() {
      return new PatientRecordWithScore(toPatientRecord(), this.score());
   }

}
