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
         LinkerProbabilistic.getProbability(currentFields.phoneticGivenName),
         LinkerProbabilistic.getProbability(currentFields.phoneticFamilyName),
         LinkerProbabilistic.getProbability(currentFields.gender),
         LinkerProbabilistic.getProbability(currentFields.dob),
         LinkerProbabilistic.getProbability(currentFields.nupi));
   }

   private record Fields(
         LinkerProbabilistic.Field phoneticGivenName,
         LinkerProbabilistic.Field phoneticFamilyName,
         LinkerProbabilistic.Field gender,
         LinkerProbabilistic.Field dob,
         LinkerProbabilistic.Field nupi) {
   }

   static Fields currentFields =
      new Fields(new LinkerProbabilistic.Field(JARO_WINKLER_SIMILARITY, List.of(0.92F), 0.9F, 0.2F),
                 new LinkerProbabilistic.Field(JARO_WINKLER_SIMILARITY, List.of(0.92F), 0.9F, 0.2F),
                 new LinkerProbabilistic.Field(JARO_WINKLER_SIMILARITY, List.of(0.92F), 0.9F, 0.5F),
                 new LinkerProbabilistic.Field(JARO_WINKLER_SIMILARITY, List.of(0.92F), 0.7428104F, 4.52E-5F),
                 new LinkerProbabilistic.Field(JARO_WINKLER_SIMILARITY, List.of(0.92F), 0.97F, 1.0E-7F));

   public static float probabilisticScore(
         final CustomDemographicData goldenRecord,
         final CustomDemographicData interaction) {
      // min, max, score, missingPenalty
      final float[] metrics = {0, 0, 0, 1.0F};
      LinkerProbabilistic.updateMetricsForStringField(metrics,
                                                      goldenRecord.phoneticGivenName, interaction.phoneticGivenName, currentFields.phoneticGivenName);
      LinkerProbabilistic.updateMetricsForStringField(metrics,
                                                      goldenRecord.phoneticFamilyName, interaction.phoneticFamilyName, currentFields.phoneticFamilyName);
      LinkerProbabilistic.updateMetricsForStringField(metrics,
                                                      goldenRecord.gender, interaction.gender, currentFields.gender);
      LinkerProbabilistic.updateMetricsForStringField(metrics,
                                                      goldenRecord.dob, interaction.dob, currentFields.dob);
      LinkerProbabilistic.updateMetricsForStringField(metrics,
                                                      goldenRecord.nupi, interaction.nupi, currentFields.nupi);
      return ((metrics[2] - metrics[0]) / (metrics[1] - metrics[0])) * metrics[3];
   }

   public static void updateMU(final CustomMU mu) {
      if (mu.phoneticGivenName().m() > mu.phoneticGivenName().u()
          && mu.phoneticFamilyName().m() > mu.phoneticFamilyName().u()
          && mu.gender().m() > mu.gender().u()
          && mu.dob().m() > mu.dob().u()
          && mu.nupi().m() > mu.nupi().u()) {
         updatedFields = new Fields(
            new LinkerProbabilistic.Field(JARO_WINKLER_SIMILARITY, List.of(0.92F), mu.phoneticGivenName().m(), mu.phoneticGivenName().u()),
            new LinkerProbabilistic.Field(JARO_WINKLER_SIMILARITY, List.of(0.92F), mu.phoneticFamilyName().m(), mu.phoneticFamilyName().u()),
            new LinkerProbabilistic.Field(JARO_WINKLER_SIMILARITY, List.of(0.92F), mu.gender().m(), mu.gender().u()),
            new LinkerProbabilistic.Field(JARO_WINKLER_SIMILARITY, List.of(0.92F), mu.dob().m(), mu.dob().u()),
            new LinkerProbabilistic.Field(JARO_WINKLER_SIMILARITY, List.of(0.92F), mu.nupi().m(), mu.nupi().u()));
      }
   }

}
