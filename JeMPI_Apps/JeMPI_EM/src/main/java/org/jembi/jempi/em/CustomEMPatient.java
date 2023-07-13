package org.jembi.jempi.em;

import org.jembi.jempi.shared.models.CustomDemographicData;

record CustomEMPatient(
      String col1,
      String col1Phonetic,
      String col2,
      String col2Phonetic,
      String genderAtBirth,
      String dateOfBirth,
      String city,
      String cityPhonetic,
      String phoneNumber,
      String nationalID) {

   CustomEMPatient(final CustomDemographicData patient) {
      this(null, null,
              null, null,
              null,
              null,
              null,
              null, // patient.city, CustomEMTask.getPhonetic(patient.city),
              null, // patient.phoneNumber,
              null);
//      this(patient.givenName, CustomEMTask.getPhonetic(patient.givenName),
//           patient.familyName, CustomEMTask.getPhonetic(patient.familyName),
//           patient.gender,
//           patient.dob,
//           StringUtils.EMPTY, StringUtils.EMPTY, // patient.city, CustomEMTask.getPhonetic(patient.city),
//           StringUtils.EMPTY, // patient.phoneNumber,
//           null);
   }
}


