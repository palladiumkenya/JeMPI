package org.jembi.jempi.shared.config;

import org.apache.commons.lang3.tuple.Pair;
import org.jembi.jempi.shared.config.dgraph.*;
import org.jembi.jempi.shared.config.input.JsonConfig;

import java.util.List;

public class DGraphConfig {

   public final List<Pair<String, Integer>> demographicDataFields;
   public final String mutationCreateInteractionFields;
   public final String mutationCreateInteractionType;
   public final String mutationCreateGoldenRecordFields;
   public final String mutationCreateGoldenRecordType;

   DGraphConfig(final JsonConfig jsonConfig) {
      demographicDataFields = DemographicDataFields.create(jsonConfig);
      mutationCreateInteractionFields = MutationCreateInteractionFields.create(jsonConfig);
      mutationCreateInteractionType = MutationCreateInteractionType.create(jsonConfig);
      mutationCreateGoldenRecordFields = MutationCreateGoldenRecordFields.create(jsonConfig);
      mutationCreateGoldenRecordType = MutationCreateGoldenRecordType.create(jsonConfig);
   }

}
