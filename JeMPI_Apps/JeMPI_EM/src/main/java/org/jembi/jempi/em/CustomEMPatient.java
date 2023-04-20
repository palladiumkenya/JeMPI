package org.jembi.jempi.em;

import org.jembi.jempi.shared.models.CustomDemographicData;

record CustomEMPatient(
      String patientPkv,
      String patientPk,
      String siteCode,
      String genderAtBirth,
      String dateOfBirth,
      String nationalID) {

   CustomEMPatient(final CustomDemographicData patient) {
      this(null,
           null,
           null,
           null,
           null,
           null);
   }
}


