package org.jembi.jempi.linker.backend;

import org.apache.commons.lang3.StringUtils;

import org.jembi.jempi.shared.models.CustomDemographicData;

final class CustomLinkerDeterministic {

   private CustomLinkerDeterministic() {
   }

   private static boolean isMatch(
         final String left,
         final String right) {
      return StringUtils.isNotBlank(left) && StringUtils.equals(left, right);
   }

   static boolean deterministicMatch(
         final CustomDemographicData goldenRecord,
         final CustomDemographicData interaction) {
      final var nupiL = goldenRecord.nupi;
      final var nupiR = interaction.nupi;
      if (isMatch(nupiL, nupiR)) {
         return true;
      }
      final var givenNameL = goldenRecord.givenName;
      final var givenNameR = interaction.givenName;
      final var familyNameL = goldenRecord.familyName;
      final var familyNameR = interaction.familyName;
      final var genderL = goldenRecord.gender;
      final var genderR = interaction.gender;
      final var dobL = goldenRecord.dob;
      final var dobR = interaction.dob;
      return (isMatch(givenNameL, givenNameR) && isMatch(familyNameL, familyNameR) && isMatch(genderL, genderR) && isMatch(dobL, dobR));
   }

}
