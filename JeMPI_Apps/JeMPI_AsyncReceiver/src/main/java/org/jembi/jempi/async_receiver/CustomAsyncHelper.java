package org.jembi.jempi.async_receiver;

import org.apache.commons.csv.CSVRecord;
import org.jembi.jempi.shared.models.CustomDemographicData;
import org.jembi.jempi.shared.models.CustomSourceId;
import org.jembi.jempi.shared.models.CustomUniqueInteractionData;

final class CustomAsyncHelper {

   private static final int AUX_ID_COL_NUM = 0;
   private static final int AUX_CLINICAL_DATA_COL_NUM = 8;
   private static final int SOURCEID_FACILITY_COL_NUM = 6;
   private static final int SOURCEID_PATIENT_COL_NUM = 7;
   private static final int GIVEN_NAME_COL_NUM = 1;
   private static final int FAMILY_NAME_COL_NUM = 2;
   private static final int GENDER_COL_NUM = 3;
   private static final int DOB_COL_NUM = 4;
   private static final int NUPI_COL_NUM = 5;

   private CustomAsyncHelper() {
   }

   static CustomUniqueInteractionData customUniqueInteractionData(final CSVRecord csvRecord, final String dwhId) {
      return new CustomUniqueInteractionData(java.time.LocalDateTime.now(),
                                             Main.parseRecordNumber(csvRecord.get(AUX_ID_COL_NUM)),
                                             csvRecord.get(AUX_CLINICAL_DATA_COL_NUM));
   }

   static CustomDemographicData customDemographicData(final CSVRecord csvRecord) {
      return new CustomDemographicData(
         csvRecord.get(GIVEN_NAME_COL_NUM),
         csvRecord.get(FAMILY_NAME_COL_NUM),
         csvRecord.get(GENDER_COL_NUM),
         csvRecord.get(DOB_COL_NUM),
         csvRecord.get(NUPI_COL_NUM));
   }

   static CustomSourceId customSourceId(final CSVRecord csvRecord) {
      return new CustomSourceId(
         null,
         csvRecord.get(SOURCEID_FACILITY_COL_NUM),
         csvRecord.get(SOURCEID_PATIENT_COL_NUM));
   }

}

