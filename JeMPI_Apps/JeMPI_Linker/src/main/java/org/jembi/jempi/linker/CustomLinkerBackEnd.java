package org.jembi.jempi.linker;

import org.jembi.jempi.libmpi.LibMPI;
import org.jembi.jempi.shared.models.CustomDemographicData;

import java.util.List;

public final class CustomLinkerBackEnd {

   private CustomLinkerBackEnd() {
   }

   static void updateGoldenRecordFields(
         final LibMPI libMPI,
         final String goldenId) {
      final var expandedGoldenRecord = libMPI.findExpandedGoldenRecords(List.of(goldenId)).get(0);
      final var goldenRecord = expandedGoldenRecord.goldenRecord();
      final var demographicData = goldenRecord.demographicData();
      var k = 0;

      k += BackEnd.updateGoldenRecordField(expandedGoldenRecord,
                                           "givenName", demographicData.givenName(), CustomDemographicData::givenName)
            ? 1
            : 0;
      k += BackEnd.updateGoldenRecordField(expandedGoldenRecord,
                                           "familyName", demographicData.familyName(), CustomDemographicData::familyName)
            ? 1
            : 0;
      k += BackEnd.updateGoldenRecordField(expandedGoldenRecord,
                                           "gender", demographicData.gender(), CustomDemographicData::gender)
            ? 1
            : 0;
      k += BackEnd.updateGoldenRecordField(expandedGoldenRecord,
                                           "dob", demographicData.dob(), CustomDemographicData::dob)
            ? 1
            : 0;
      k += BackEnd.updateGoldenRecordField(expandedGoldenRecord,
                                           "nationalId", demographicData.nationalId(), CustomDemographicData::nationalId)
            ? 1
            : 0;

      if (k > 0) {
        BackEnd.updateMatchingPatientRecordScoreForGoldenRecord(expandedGoldenRecord);
      }

   }

}
