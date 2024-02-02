package org.jembi.jempi.linker.backend;

import org.apache.commons.lang3.StringUtils;

import org.jembi.jempi.shared.models.CustomDemographicData;

final class CustomLinkerDeterministic {

   static final boolean DETERMINISTIC_DO_LINKING = true;
   static final boolean DETERMINISTIC_DO_VALIDATING = true;
   static final boolean DETERMINISTIC_DO_MATCHING = true;

   private CustomLinkerDeterministic() {
   }

   private static boolean isMatch(
         final String left,
         final String right) {
      return StringUtils.isNotBlank(left) && StringUtils.equals(left, right);
   }

   static boolean canApplyLinking(
         final CustomDemographicData interaction) {
      return CustomLinkerProbabilistic.PROBABILISTIC_DO_LINKING
             || StringUtils.isNotBlank(interaction.nupi);
   }

   static boolean linkDeterministicMatch(
         final CustomDemographicData goldenRecord,
         final CustomDemographicData interaction) {
      final var nupiL = goldenRecord.nupi;
      final var nupiR = interaction.nupi;
      return isMatch(nupiL, nupiR);
   }

   static boolean validateDeterministicMatch(
         final CustomDemographicData goldenRecord,
         final CustomDemographicData interaction) {
      final var cccNumberL = goldenRecord.cccNumber;
      final var cccNumberR = interaction.cccNumber;
      return isMatch(cccNumberL, cccNumberR);
   }

   static boolean matchNotificationDeterministicMatch(
         final CustomDemographicData goldenRecord,
         final CustomDemographicData interaction) {
      final var givenNameL = goldenRecord.givenName;
      final var givenNameR = interaction.givenName;
      final var familyNameL = goldenRecord.familyName;
      final var familyNameR = interaction.familyName;
      final var dobL = goldenRecord.dob;
      final var dobR = interaction.dob;
      final var genderL = goldenRecord.gender;
      final var genderR = interaction.gender;
      return (isMatch(givenNameL, givenNameR) && isMatch(familyNameL, familyNameR) && isMatch(dobL, dobR) && isMatch(genderL, genderR));
   }

}
