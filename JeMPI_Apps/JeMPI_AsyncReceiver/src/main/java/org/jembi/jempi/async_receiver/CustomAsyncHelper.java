package org.jembi.jempi.async_receiver;

import org.apache.commons.csv.CSVRecord;
import org.jembi.jempi.shared.models.CustomDemographicData;
import org.jembi.jempi.shared.models.CustomSourceId;
import org.jembi.jempi.shared.models.CustomUniqueInteractionData;

final class CustomAsyncHelper {

   private static final int CCC_NUMBER_COL_NUM = 6;
   private static final int PKV_COL_NUM = 0;
   private static final int SOURCEID_FACILITY_COL_NUM = 4;
   private static final int SOURCEID_PATIENT_COL_NUM = 5;
   private static final int GENDER_COL_NUM = 1;
   private static final int DOB_COL_NUM = 2;
   private static final int NUPI_COL_NUM = 3;


   private CustomAsyncHelper() {
   }

   static CustomUniqueInteractionData customUniqueInteractionData(final CSVRecord csvRecord, final String dwhId) {
      return new CustomUniqueInteractionData(java.time.LocalDateTime.now(),
                                             null,
                                             csvRecord.get(CCC_NUMBER_COL_NUM),
                                             csvRecord.get(PKV_COL_NUM),
                                             dwhId);
   }

   static CustomDemographicData customDemographicData(final CSVRecord csvRecord) {
      return new CustomDemographicData(
         null,
         null,
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

