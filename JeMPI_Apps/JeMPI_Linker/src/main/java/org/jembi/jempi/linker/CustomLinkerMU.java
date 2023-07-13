package org.jembi.jempi.linker;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.text.similarity.SimilarityScore;
import org.apache.commons.text.similarity.JaroWinklerSimilarity;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jembi.jempi.shared.models.CustomDemographicData;


public final class CustomLinkerMU {

   private static final Logger LOGGER = LogManager.getLogger(CustomLinkerMU.class);
   private static final JaroWinklerSimilarity JARO_WINKLER_SIMILARITY = new JaroWinklerSimilarity();

   private final Fields fields = new Fields();

   CustomLinkerMU() {
      LOGGER.debug("CustomLinkerMU");
   }

   private static boolean fieldMismatch(
         final Field field,
         final String left,
         final String right) {
      return field.similarityScore.apply(left, right) <= field.threshold;
   }

   private void updateMatchedPair(
         final Field field,
         final String left,
         final String right) {
      if (StringUtils.isBlank(left) || StringUtils.isBlank(right) || fieldMismatch(field, left, right)) {
         field.matchedPairFieldUnmatched += 1;
      } else {
         field.matchedPairFieldMatched += 1;
      }
   }

   private void updateUnMatchedPair(
         final Field field,
         final String left,
         final String right) {
      if (StringUtils.isBlank(left) || StringUtils.isBlank(right) || fieldMismatch(field, left, right)) {
         field.unMatchedPairFieldUnmatched += 1;
      } else {
         field.unMatchedPairFieldMatched += 1;
      }
   }

   void updateMatchSums(
         final CustomDemographicData patient,
         final CustomDemographicData goldenRecord) {
      updateMatchedPair(fields.phoneticGivenName, patient.phoneticGivenName, goldenRecord.phoneticGivenName);
      updateMatchedPair(fields.phoneticFamilyName, patient.phoneticFamilyName, goldenRecord.phoneticFamilyName);
      updateMatchedPair(fields.gender, patient.gender, goldenRecord.gender);
      updateMatchedPair(fields.dob, patient.dob, goldenRecord.dob);
      updateMatchedPair(fields.nupi, patient.nupi, goldenRecord.nupi);
      LOGGER.debug("{}", fields);
   }

   void updateMissmatchSums(
         final CustomDemographicData patient,
         final CustomDemographicData goldenRecord) {
      updateUnMatchedPair(fields.phoneticGivenName, patient.phoneticGivenName, goldenRecord.phoneticGivenName);
      updateUnMatchedPair(fields.phoneticFamilyName, patient.phoneticFamilyName, goldenRecord.phoneticFamilyName);
      updateUnMatchedPair(fields.gender, patient.gender, goldenRecord.gender);
      updateUnMatchedPair(fields.dob, patient.dob, goldenRecord.dob);
      updateUnMatchedPair(fields.nupi, patient.nupi, goldenRecord.nupi);
      LOGGER.debug("{}", fields);
   }

   static class Field {
      final SimilarityScore<Double> similarityScore;
      final double threshold;
      long matchedPairFieldMatched = 0L;
      long matchedPairFieldUnmatched = 0L;
      long unMatchedPairFieldMatched = 0L;
      long unMatchedPairFieldUnmatched = 0L;

      Field(final SimilarityScore<Double> score,
            final double mismatchThreshold) {
         this.similarityScore = score;
         this.threshold = mismatchThreshold;
      }
   }

   static class Fields {
      final Field phoneticGivenName = new Field(JARO_WINKLER_SIMILARITY, 0.92);
      final Field phoneticFamilyName = new Field(JARO_WINKLER_SIMILARITY, 0.92);
      final Field gender = new Field(JARO_WINKLER_SIMILARITY, 0.92);
      final Field dob = new Field(JARO_WINKLER_SIMILARITY, 0.92);
      final Field nupi = new Field(JARO_WINKLER_SIMILARITY, 0.92);

      private float computeM(final Field field) {
         return (float) (field.matchedPairFieldMatched)
              / (float) (field.matchedPairFieldMatched + field.matchedPairFieldUnmatched);
      }

      private float computeU(final Field field) {
         return (float) (field.unMatchedPairFieldMatched)
              / (float) (field.unMatchedPairFieldMatched + field.unMatchedPairFieldUnmatched);
      }

      @Override
      public String toString() {
         return String.format("f1(%f:%f) f2(%f:%f) f3(%f:%f) f4(%f:%f) f5(%f:%f)",
                              computeM(phoneticGivenName), computeU(phoneticGivenName),
                              computeM(phoneticFamilyName), computeU(phoneticFamilyName),
                              computeM(gender), computeU(gender),
                              computeM(dob), computeU(dob),
                              computeM(nupi), computeU(nupi));
      }

   }

}
