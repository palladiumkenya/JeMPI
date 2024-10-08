package org.jembi.jempi.libmpi.dgraph;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.jembi.jempi.shared.models.InteractionWithScore;
import org.jembi.jempi.shared.models.CustomUniqueInteractionData;
import org.jembi.jempi.shared.models.CustomDemographicData;
import org.jembi.jempi.shared.models.Interaction;

@JsonInclude(JsonInclude.Include.NON_NULL)
record CustomDgraphInteraction(
      @JsonProperty("uid") String interactionId,
      @JsonProperty("Interaction.source_id") DgraphSourceId sourceId,
      @JsonProperty(CustomDgraphConstants.PREDICATE_INTERACTION_AUX_DATE_CREATED) java.time.LocalDateTime auxDateCreated,
      @JsonProperty(CustomDgraphConstants.PREDICATE_INTERACTION_AUX_ID) String auxId,
      @JsonProperty(CustomDgraphConstants.PREDICATE_INTERACTION_PKV) String pkv,
      @JsonProperty(CustomDgraphConstants.PREDICATE_INTERACTION_AUX_DWH_ID) String auxDwhId,
      @JsonProperty(CustomDgraphConstants.PREDICATE_INTERACTION_GIVEN_NAME) String givenName,
      @JsonProperty(CustomDgraphConstants.PREDICATE_INTERACTION_FAMILY_NAME) String familyName,
      @JsonProperty(CustomDgraphConstants.PREDICATE_INTERACTION_GENDER) String gender,
      @JsonProperty(CustomDgraphConstants.PREDICATE_INTERACTION_DOB) String dob,
      @JsonProperty(CustomDgraphConstants.PREDICATE_INTERACTION_NUPI) String nupi,
      @JsonProperty(CustomDgraphConstants.PREDICATE_INTERACTION_CCC_NUMBER) String cccNumber,
      @JsonProperty(CustomDgraphConstants.PREDICATE_INTERACTION_DOCKET) String docket,
      @JsonProperty("GoldenRecord.interactions|score") Float score) {

   CustomDgraphInteraction(
         final Interaction interaction,
         final Float score) {
      this(interaction.interactionId(),
           new DgraphSourceId(interaction.sourceId()),
           interaction.uniqueInteractionData().auxDateCreated(),
           interaction.uniqueInteractionData().auxId(),
           interaction.uniqueInteractionData().pkv(),
           interaction.uniqueInteractionData().auxDwhId(),
           interaction.demographicData().givenName,
           interaction.demographicData().familyName,
           interaction.demographicData().gender,
           interaction.demographicData().dob,
           interaction.demographicData().nupi,
           interaction.demographicData().cccNumber,
           interaction.demographicData().docket,
           score);
   }

   Interaction toInteraction() {
      return new Interaction(this.interactionId(),
                             this.sourceId() != null
                                   ? this.sourceId().toSourceId()
                                   : null,
                             new CustomUniqueInteractionData(this.auxDateCreated,
                                                               this.auxId,
                                                               this.pkv,
                                                               this.auxDwhId),
                             new CustomDemographicData(this.givenName,
                                                       this.familyName,
                                                       this.gender,
                                                       this.dob,
                                                       this.nupi,
                                                       this.cccNumber,
                                                       this.docket));
   }

   InteractionWithScore toInteractionWithScore() {
      return new InteractionWithScore(toInteraction(), this.score());
   }

}

