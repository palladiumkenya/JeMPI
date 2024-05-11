package org.jembi.jempi.libmpi.dgraph;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jembi.jempi.shared.models.Interaction;
import org.jembi.jempi.shared.models.InteractionWithScore;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.jembi.jempi.shared.utils.AppUtils.OBJECT_MAPPER;

@JsonInclude(JsonInclude.Include.NON_NULL)
record JsonNodeInteractions(
      JsonNode all) {

   private static final Logger LOGGER = LogManager.getLogger(JsonNodeInteractions.class);

   JsonNodeInteractions(final String json) throws JsonProcessingException {
      this(toJsonNode(json));
   }

   private static JsonNode toJsonNode(final String json) throws JsonProcessingException {
      LOGGER.debug("{}", json);
      return OBJECT_MAPPER.readTree(json).get("all");
   }

   List<InteractionWithScore> toInteractionsWithScore() {
      final List<InteractionWithScore> interactions = new ArrayList<>();
      final Iterator<JsonNode> iter = all.elements();
      while (iter.hasNext()) {
         final var next = iter.next();
         interactions.add(JsonNodeInteraction.toInteractionWithScore(next));
      }
      return interactions;
   }

   List<Interaction> toInteractions() {
      final List<Interaction> interactions = new ArrayList<>();
      final Iterator<JsonNode> iter = all.elements();
      while (iter.hasNext()) {
         final var next = iter.next();
         interactions.add(JsonNodeInteraction.toInteraction(next));
      }
      return interactions;
   }

}
