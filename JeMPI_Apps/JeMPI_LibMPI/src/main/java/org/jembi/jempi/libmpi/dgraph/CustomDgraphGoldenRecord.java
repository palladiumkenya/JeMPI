package org.jembi.jempi.libmpi.dgraph;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jembi.jempi.shared.models.CustomDemographicData;
import org.jembi.jempi.shared.models.GoldenRecord;

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
record CustomDgraphGoldenRecord(
      @JsonProperty("uid") String goldenId,
      @JsonProperty("GoldenRecord.source_id") List<DgraphSourceId> sourceId,
      @JsonProperty("GoldenRecord.aux_id") String auxId,
      @JsonProperty("GoldenRecord.aux_dwh_id") String auxDwhId,
      @JsonProperty("GoldenRecord.patient_pkv") String patientPkv,
      @JsonProperty("GoldenRecord.site_code") String siteCode,
      @JsonProperty("GoldenRecord.patient_pk") String patientPk,
      @JsonProperty("GoldenRecord.dob") String dob,
      @JsonProperty("GoldenRecord.nupi") String nupi) {

   CustomDgraphGoldenRecord(final CustomDgraphPatientRecord rec) {
      this(null,
           List.of(rec.sourceId()),
           rec.auxId(),
           rec.auxDwhId(),
           rec.patientPkv(),
           rec.siteCode(),
           rec.patientPk(),
           rec.dob(),
           rec.nupi());
   }

   GoldenRecord toGoldenRecord() {
      return new GoldenRecord(this.goldenId(),
                              this.sourceId() != null
                                    ? this.sourceId().stream().map(DgraphSourceId::toSourceId).toList()
                                    : List.of(),
                              new CustomDemographicData(this.auxId(),
                                                        this.auxDwhId(),
                                                        this.patientPkv(),
                                                        this.siteCode(),
                                                        this.patientPk(),
                                                        this.dob(),
                                                        this.nupi()));
   }

}
