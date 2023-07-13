package org.jembi.jempi.libmpi.postgresql;

import org.jembi.jempi.shared.models.CustomDemographicData;

final class CustomInteractionData extends CustomDemographicData implements NodeData {

   CustomInteractionData(final CustomDemographicData customDemographicData) {
      super(customDemographicData.phoneticGivenName,
            customDemographicData.phoneticFamilyName,
            customDemographicData.gender,
            customDemographicData.dob,
            customDemographicData.nupi,
            customDemographicData.cccNumber);
   }

}

