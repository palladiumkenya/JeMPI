package org.jembi.jempi.linker.backend;

import org.jembi.jempi.libmpi.LibMPI;
import org.jembi.jempi.shared.models.CustomDemographicData;

import java.util.List;

public final class CustomLinkerBackEnd {

   private CustomLinkerBackEnd() {
   }

   static void updateGoldenRecordFields(
         final BackEnd backEnd,
         final LibMPI libMPI,
         final float threshold,
         final String interactionId,
         final String goldenId) {
      final var expandedGoldenRecord = libMPI.findExpandedGoldenRecords(List.of(goldenId)).get(0);
      final var goldenRecord = expandedGoldenRecord.goldenRecord();
      final var demographicData = goldenRecord.demographicData();
      var k = 0;

      k += backEnd.helperUpdateGoldenRecordField(interactionId, expandedGoldenRecord,
                                                 "phoneticGivenName", demographicData.phoneticGivenName, CustomDemographicData::getPhoneticGivenName)
            ? 1
            : 0;
      k += backEnd.helperUpdateGoldenRecordField(interactionId, expandedGoldenRecord,
                                                 "phoneticFamilyName", demographicData.phoneticFamilyName, CustomDemographicData::getPhoneticFamilyName)
            ? 1
            : 0;
      k += backEnd.helperUpdateGoldenRecordField(interactionId, expandedGoldenRecord,
                                                 "gender", demographicData.gender, CustomDemographicData::getGender)
            ? 1
            : 0;
      k += backEnd.helperUpdateGoldenRecordField(interactionId, expandedGoldenRecord,
                                                 "dob", demographicData.dob, CustomDemographicData::getDob)
            ? 1
            : 0;
      k += backEnd.helperUpdateGoldenRecordField(interactionId, expandedGoldenRecord,
                                                 "nupi", demographicData.nupi, CustomDemographicData::getNupi)
            ? 1
            : 0;

      if (k > 0) {
        backEnd.helperUpdateInteractionsScore(threshold, expandedGoldenRecord);
      }

   }

}
