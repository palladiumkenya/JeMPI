package org.jembi.jempi.linker;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.similarity.JaroWinklerSimilarity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jembi.jempi.shared.models.CustomDemographicData;
import org.jembi.jempi.shared.models.CustomMU;

import static java.lang.Math.log;

public final class CustomLinkerProbabilistic {

   private static final Logger LOGGER = LogManager.getLogger(CustomLinkerProbabilistic.class);
   private static final JaroWinklerSimilarity JARO_WINKLER_SIMILARITY = new JaroWinklerSimilarity();
   private static final double LOG2 = java.lang.Math.log(2.0);
   private static final float MISSING_PENALTY = 0.925F;
   private static Fields updatedFields = null;

   private CustomLinkerProbabilistic() {
   }

   private static float limitProbability(final float p) {
      if (p > 1.0F - 1E-5F) {
         return 1.0F - 1E-5F;
      } else if (p < 1E-5F) {
         return 1E-5F;
      }
      return p;
   }

   private static float fieldScore(
         final boolean match,
         final float m,
         final float u) {
      if (match) {
         return (float) (log(m / u) / LOG2);
      }
      return (float) (log((1.0 - m) / (1.0 - u)) / LOG2);
   }

   private static float fieldScore(
         final String left,
         final String right,
         final Field field) {
      return fieldScore(JARO_WINKLER_SIMILARITY.apply(left, right) > 0.92, field.m, field.u);
   }

   private static CustomMU.Probability getProbability(final Field field) {
      return new CustomMU.Probability(field.m(), field.u());
   }

   public static void checkUpdatedMU() {
      if (updatedFields != null) {
         LOGGER.info("Using updated MU values: {}", updatedFields);
         currentFields = updatedFields;
         updatedFields = null;
      }
   }

   private record Field(float m, float u, float min, float max) {
      Field {
         m = limitProbability(m);
         u = limitProbability(u);
         min = fieldScore(false, m, u);
         max = fieldScore(true, m, u);
      }

      Field(final float m_, final float u_) {
         this(m_, u_, 0.0F, 0.0F);
      }

   }

   private static void updateMetricsForStringField(
         final float[] metrics,
         final String left,
         final String right,
         final Field field) {
      if (StringUtils.isNotBlank(left) && StringUtils.isNotBlank(right)) {
         metrics[0] += field.min;
         metrics[1] += field.max;
         metrics[2] += fieldScore(left, right, field);
      } else {
         metrics[3] *= MISSING_PENALTY;
      }
   }

   static CustomMU getMU() {
      return new CustomMU(
         getProbability(currentFields.phoneticGivenName),
         getProbability(currentFields.phoneticFamilyName),
         getProbability(currentFields.gender),
         getProbability(currentFields.dob),
         getProbability(currentFields.nupi));
   }

   private record Fields(
         Field phoneticGivenName,
         Field phoneticFamilyName,
         Field gender,
         Field dob,
         Field nupi) {
   }

   private static Fields currentFields =
      new Fields(new Field(0.782501F, 0.02372F),
                 new Field(0.850909F, 0.02975F),
                 new Field(0.786614F, 0.443018F),
                 new Field(0.894637F, 0.012448F),
                 new Field(0.832336F, 1.33E-4F));

   public static float probabilisticScore(
         final CustomDemographicData goldenRecord,
         final CustomDemographicData patient) {
      // min, max, score, missingPenalty
      final float[] metrics = {0, 0, 0, 1.0F};
      updateMetricsForStringField(metrics,
                                  goldenRecord.phoneticGivenName(), patient.phoneticGivenName(), currentFields.phoneticGivenName);
      updateMetricsForStringField(metrics,
                                  goldenRecord.phoneticFamilyName(), patient.phoneticFamilyName(), currentFields.phoneticFamilyName);
      updateMetricsForStringField(metrics,
                                  goldenRecord.gender(), patient.gender(), currentFields.gender);
      updateMetricsForStringField(metrics,
                                  goldenRecord.dob(), patient.dob(), currentFields.dob);
      updateMetricsForStringField(metrics,
                                  goldenRecord.nupi(), patient.nupi(), currentFields.nupi);
      return ((metrics[2] - metrics[0]) / (metrics[1] - metrics[0])) * metrics[3];
   }

   public static void updateMU(final CustomMU mu) {
      if (mu.phoneticGivenName().m() > mu.phoneticGivenName().u()
          && mu.phoneticFamilyName().m() > mu.phoneticFamilyName().u()
          && mu.gender().m() > mu.gender().u()
          && mu.dob().m() > mu.dob().u()
          && mu.nupi().m() > mu.nupi().u()) {
         updatedFields = new Fields(
            new Field(mu.phoneticGivenName().m(), mu.phoneticGivenName().u()),
            new Field(mu.phoneticFamilyName().m(), mu.phoneticFamilyName().u()),
            new Field(mu.gender().m(), mu.gender().u()),
            new Field(mu.dob().m(), mu.dob().u()),
            new Field(mu.nupi().m(), mu.nupi().u()));
      }
   }

}
