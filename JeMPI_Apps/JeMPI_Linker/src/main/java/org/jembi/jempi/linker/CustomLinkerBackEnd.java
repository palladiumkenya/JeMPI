package org.jembi.jempi.linker;

import org.jembi.jempi.libmpi.LibMPI;
import org.jembi.jempi.shared.models.CustomDemographicData;

import java.util.List;

public final class CustomLinkerBackEnd {

   private CustomLinkerBackEnd() {
   }

   static void updateGoldenRecordFields(
         final BackEnd backEnd,
         final LibMPI libMPI,
         final String goldenId) {
      final var expandedGoldenRecord = libMPI.findExpandedGoldenRecords(List.of(goldenId)).get(0);
      final var goldenRecord = expandedGoldenRecord.goldenRecord();
      final var demographicData = goldenRecord.demographicData();
      var k = 0;

      k += backEnd.updateGoldenRecordField(expandedGoldenRecord,
                                           "phoneticGivenName", demographicData.phoneticGivenName(), CustomDemographicData::phoneticGivenName)
            ? 1
            : 0;
      k += backEnd.updateGoldenRecordField(expandedGoldenRecord,
                                           "phoneticFamilyName", demographicData.phoneticFamilyName(), CustomDemographicData::phoneticFamilyName)
            ? 1
            : 0;
      k += backEnd.updateGoldenRecordField(expandedGoldenRecord,
                                           "gender", demographicData.gender(), CustomDemographicData::gender)
            ? 1
            : 0;
      k += backEnd.updateGoldenRecordField(expandedGoldenRecord,
                                           "dob", demographicData.dob(), CustomDemographicData::dob)
            ? 1
            : 0;
      k += backEnd.updateGoldenRecordField(expandedGoldenRecord,
                                           "nupi", demographicData.nupi(), CustomDemographicData::nupi)
            ? 1
            : 0;

      if (k > 0) {
        backEnd.updateMatchingPatientRecordScoreForGoldenRecord(expandedGoldenRecord);
      }

   }

}
