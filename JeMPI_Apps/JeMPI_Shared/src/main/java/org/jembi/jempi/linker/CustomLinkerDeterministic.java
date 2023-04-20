package org.jembi.jempi.linker;

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
         final CustomDemographicData patient) {
      final var phoneticGivenNameL = goldenRecord.phoneticGivenName();
      final var phoneticGivenNameR = patient.phoneticGivenName();
      final var phoneticFamilyNameL = goldenRecord.phoneticFamilyName();
      final var phoneticFamilyNameR = patient.phoneticFamilyName();
      final var genderL = goldenRecord.gender();
      final var genderR = patient.gender();
      final var dobL = goldenRecord.dob();
      final var dobR = patient.dob();
      final var nupiL = goldenRecord.nupi();
      final var nupiR = patient.nupi();
      return (isMatch(nupiL, nupiR) || (isMatch(phoneticGivenNameL, phoneticGivenNameR) && isMatch(phoneticFamilyNameL, phoneticFamilyNameR) && isMatch(genderL, genderR) && isMatch(dobL, dobR)));
   }

}
