package org.jembi.jempi.libmpi.dgraph;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jembi.jempi.shared.models.CustomUniqueGoldenRecordData;
import org.jembi.jempi.shared.models.CustomDemographicData;
import org.jembi.jempi.shared.models.GoldenRecord;

import java.util.List;

@JsonInclude(JsonInclude.Include.NON_NULL)
record CustomDgraphGoldenRecord(
      @JsonProperty("uid") String goldenId,
      @JsonProperty("GoldenRecord.source_id") List<DgraphSourceId> sourceId,
      @JsonProperty(CustomDgraphConstants.PREDICATE_GOLDEN_RECORD_AUX_DATE_CREATED) java.time.LocalDateTime auxDateCreated,
      @JsonProperty(CustomDgraphConstants.PREDICATE_GOLDEN_RECORD_AUX_AUTO_UPDATE_ENABLED) Boolean auxAutoUpdateEnabled,
      @JsonProperty(CustomDgraphConstants.PREDICATE_GOLDEN_RECORD_AUX_ID) String auxId,
      @JsonProperty(CustomDgraphConstants.PREDICATE_GOLDEN_RECORD_GIVEN_NAME) String givenName,
      @JsonProperty(CustomDgraphConstants.PREDICATE_GOLDEN_RECORD_FAMILY_NAME) String familyName,
      @JsonProperty(CustomDgraphConstants.PREDICATE_GOLDEN_RECORD_GENDER) String gender,
      @JsonProperty(CustomDgraphConstants.PREDICATE_GOLDEN_RECORD_DOB) String dob,
      @JsonProperty(CustomDgraphConstants.PREDICATE_GOLDEN_RECORD_NUPI) String nupi,
      @JsonProperty(CustomDgraphConstants.PREDICATE_GOLDEN_RECORD_CCC_NUMBER) String cccNumber,
      @JsonProperty(CustomDgraphConstants.PREDICATE_GOLDEN_RECORD_DOCKET) String docket) {

   GoldenRecord toGoldenRecord() {
      return new GoldenRecord(this.goldenId(),
                              this.sourceId() != null
                                 ? this.sourceId().stream().map(DgraphSourceId::toSourceId).toList()
                                 : List.of(),
                              new CustomUniqueGoldenRecordData(this.auxDateCreated(),
                                                               this.auxAutoUpdateEnabled(),
                                                               this.auxId()),
                              new CustomDemographicData(this.givenName(),
                                                        this.familyName(),
                                                        this.gender(),
                                                        this.dob(),
                                                        this.nupi(),
                                                        this.cccNumber(),
                                                        this.docket()));
   }

}

