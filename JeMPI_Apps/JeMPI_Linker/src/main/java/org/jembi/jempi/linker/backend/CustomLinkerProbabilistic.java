package org.jembi.jempi.linker.backend;

import org.jembi.jempi.shared.models.CustomDemographicData;
import org.jembi.jempi.shared.models.CustomMU;

import java.util.Arrays;
import java.util.List;

import static org.jembi.jempi.linker.backend.LinkerProbabilistic.EXACT_SIMILARITY;
import static org.jembi.jempi.linker.backend.LinkerProbabilistic.JACCARD_SIMILARITY;
import static org.jembi.jempi.linker.backend.LinkerProbabilistic.JARO_SIMILARITY;
import static org.jembi.jempi.linker.backend.LinkerProbabilistic.JARO_WINKLER_SIMILARITY;

final class CustomLinkerProbabilistic {

   static Fields updatedFields = null;

   private CustomLinkerProbabilistic() {
   }

   static CustomMU getMU() {
      return new CustomMU(
         LinkerProbabilistic.getProbability(currentFields.givenName),
         LinkerProbabilistic.getProbability(currentFields.familyName),
         LinkerProbabilistic.getProbability(currentFields.gender),
         LinkerProbabilistic.getProbability(currentFields.dob),
         LinkerProbabilistic.getProbability(currentFields.nupi));
   }

   private record Fields(
         LinkerProbabilistic.Field givenName,
         LinkerProbabilistic.Field familyName,
         LinkerProbabilistic.Field gender,
         LinkerProbabilistic.Field dob,
         LinkerProbabilistic.Field nupi) {
   }

   static Fields currentFields =
      new Fields(new LinkerProbabilistic.Field(JARO_SIMILARITY, List.of(0.96F), 0.9828191F, 0.0093628F),
                 new LinkerProbabilistic.Field(JARO_SIMILARITY, List.of(0.96F), 0.9926113F, 0.0058055F),
                 new LinkerProbabilistic.Field(JARO_SIMILARITY, List.of(0.96F), 0.9999F, 0.4999997F),
                 new LinkerProbabilistic.Field(JARO_SIMILARITY, List.of(0.96F), 0.9999999F, 6.87E-5F),
                 new LinkerProbabilistic.Field(JARO_SIMILARITY, List.of(0.96F), 0.9444177F, 5.0E-7F));

   public static float probabilisticScore(
         final CustomDemographicData goldenRecord,
         final CustomDemographicData interaction) {
      // min, max, score, missingPenalty
      final float[] metrics = {0, 0, 0, 1.0F};
      LinkerProbabilistic.updateMetricsForStringField(metrics,
                                                      goldenRecord.givenName, interaction.givenName, currentFields.givenName);
      LinkerProbabilistic.updateMetricsForStringField(metrics,
                                                      goldenRecord.familyName, interaction.familyName, currentFields.familyName);
      LinkerProbabilistic.updateMetricsForStringField(metrics,
                                                      goldenRecord.gender, interaction.gender, currentFields.gender);
      LinkerProbabilistic.updateMetricsForStringField(metrics,
                                                      goldenRecord.dob, interaction.dob, currentFields.dob);
      LinkerProbabilistic.updateMetricsForStringField(metrics,
                                                      goldenRecord.nupi, interaction.nupi, currentFields.nupi);
      return ((metrics[2] - metrics[0]) / (metrics[1] - metrics[0])) * metrics[3];
   }

   public static void updateMU(final CustomMU mu) {
      if (mu.givenName().m() > mu.givenName().u()
          && mu.familyName().m() > mu.familyName().u()
          && mu.gender().m() > mu.gender().u()
          && mu.dob().m() > mu.dob().u()
          && mu.nupi().m() > mu.nupi().u()) {
         updatedFields = new Fields(
            new LinkerProbabilistic.Field(JARO_SIMILARITY, List.of(0.96F), mu.givenName().m(), mu.givenName().u()),
            new LinkerProbabilistic.Field(JARO_SIMILARITY, List.of(0.96F), mu.familyName().m(), mu.familyName().u()),
            new LinkerProbabilistic.Field(JARO_SIMILARITY, List.of(0.96F), mu.gender().m(), mu.gender().u()),
            new LinkerProbabilistic.Field(JARO_SIMILARITY, List.of(0.96F), mu.dob().m(), mu.dob().u()),
            new LinkerProbabilistic.Field(JARO_SIMILARITY, List.of(0.96F), mu.nupi().m(), mu.nupi().u()));
      }
   }

}
