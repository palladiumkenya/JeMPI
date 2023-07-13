package org.jembi.jempi.libmpi.postgresql;

import org.jembi.jempi.shared.models.CustomDemographicData;

final class CustomGoldenRecordData extends CustomDemographicData implements NodeData {

   CustomGoldenRecordData(final CustomDemographicData customDemographicData) {
      super(customDemographicData.phoneticGivenName,
            customDemographicData.phoneticFamilyName,
            customDemographicData.gender,
            customDemographicData.dob,
            customDemographicData.nupi,
            customDemographicData.cccNumber);
   }

}

