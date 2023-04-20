package org.jembi.jempi.libmpi.dgraph;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jembi.jempi.shared.models.ExpandedGoldenRecord;
import org.jembi.jempi.shared.models.CustomDemographicData;
import org.jembi.jempi.shared.models.GoldenRecord;

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
record CustomDgraphExpandedGoldenRecord(
      @JsonProperty("uid") String goldenId,
      @JsonProperty("GoldenRecord.source_id") List<DgraphSourceId> sourceId,
      @JsonProperty("GoldenRecord.aux_id") String auxId,
      @JsonProperty("GoldenRecord.aux_dwh_id") String auxDwhId,
      @JsonProperty("GoldenRecord.phonetic_given_name") String phoneticGivenName,
      @JsonProperty("GoldenRecord.phonetic_family_name") String phoneticFamilyName,
      @JsonProperty("GoldenRecord.gender") String gender,
      @JsonProperty("GoldenRecord.dob") String dob,
      @JsonProperty("GoldenRecord.nupi") String nupi,
      @JsonProperty("GoldenRecord.patients") List<CustomDgraphPatientRecord> patients) {


   GoldenRecord toGoldenRecord() {
      return new GoldenRecord(this.goldenId(),
                              this.sourceId() != null
                                    ? this.sourceId().stream().map(DgraphSourceId::toSourceId).toList()
                                    : List.of(),
                              new CustomDemographicData(this.auxId(),
                                                        this.auxDwhId(),
                                                        this.phoneticGivenName(),
                                                        this.phoneticFamilyName(),
                                                        this.gender(),
                                                        this.dob(),
                                                        this.nupi()));
   }

   ExpandedGoldenRecord toExpandedGoldenRecord() {
      return new ExpandedGoldenRecord(this.toGoldenRecord(),
                                      this.patients().stream().map(CustomDgraphPatientRecord::toPatientRecordWithScore).toList());
   }

}
