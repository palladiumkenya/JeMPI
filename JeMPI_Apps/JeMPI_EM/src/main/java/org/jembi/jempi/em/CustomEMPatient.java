package org.jembi.jempi.em;

import org.jembi.jempi.shared.models.CustomDemographicData;

record CustomEMPatient(
      String col1,
      String col1Phonetic,
      String col2,
      String col2Phonetic,
      String genderAtBirth,
      String dateOfBirth,
      String nationalID) {

   CustomEMPatient(final CustomDemographicData patient) {
      this(patient.phoneticGivenName(), CustomEMTask.getPhonetic(patient.phoneticGivenName()),
           patient.phoneticFamilyName(), CustomEMTask.getPhonetic(patient.phoneticFamilyName()),
           patient.gender(),
           patient.dob(),
           patient.nupi());
   }
}


