package org.jembi.jempi.libmpi.dgraph;

import org.jembi.jempi.shared.models.CustomDemographicData;
import org.jembi.jempi.shared.utils.AppUtils;

import java.util.UUID;

final class CustomDgraphMutations {

   private CustomDgraphMutations() {
   }

   static String createPatientTriple(
         final CustomDemographicData demographicData,
         final String sourceUID) {
      final String uuid = UUID.randomUUID().toString();
      return String.format("""
                           _:%s  <PatientRecord.source_id>                <%s>        .
                           _:%s  <PatientRecord.aux_id>                   %s          .
                           _:%s  <PatientRecord.aux_dwh_id>               %s          .
                           _:%s  <PatientRecord.phonetic_given_name>      %s          .
                           _:%s  <PatientRecord.phonetic_family_name>     %s          .
                           _:%s  <PatientRecord.gender>                   %s          .
                           _:%s  <PatientRecord.dob>                      %s          .
                           _:%s  <PatientRecord.nupi>                     %s          .
                           _:%s  <dgraph.type>                     "PatientRecord"    .
                           """,
                           uuid, sourceUID,
                           uuid, AppUtils.quotedValue(demographicData.auxId()),
                           uuid, AppUtils.quotedValue(demographicData.auxDwhId()),
                           uuid, AppUtils.quotedValue(demographicData.phoneticGivenName()),
                           uuid, AppUtils.quotedValue(demographicData.phoneticFamilyName()),
                           uuid, AppUtils.quotedValue(demographicData.gender()),
                           uuid, AppUtils.quotedValue(demographicData.dob()),
                           uuid, AppUtils.quotedValue(demographicData.nupi()),
                           uuid);
   }

   static String createLinkedGoldenRecordTriple(
         final CustomDemographicData demographicData,
         final String patientUID,
         final String sourceUID,
         final float score) {
      final String uuid = UUID.randomUUID().toString();
      return String.format("""
                           _:%s  <GoldenRecord.source_id>                     <%s>             .
                           _:%s  <GoldenRecord.aux_id>                        %s               .
                           _:%s  <GoldenRecord.aux_dwh_id>                    %s               .
                           _:%s  <GoldenRecord.phonetic_given_name>           %s               .
                           _:%s  <GoldenRecord.phonetic_family_name>          %s               .
                           _:%s  <GoldenRecord.gender>                        %s               .
                           _:%s  <GoldenRecord.dob>                           %s               .
                           _:%s  <GoldenRecord.nupi>                          %s               .
                           _:%s  <GoldenRecord.patients>                      <%s> (score=%f)  .
                           _:%s  <dgraph.type>                                "GoldenRecord"   .
                           """,
                           uuid, sourceUID,
                           uuid, AppUtils.quotedValue(demographicData.auxId()),
                           uuid, AppUtils.quotedValue(demographicData.auxDwhId()),
                           uuid, AppUtils.quotedValue(demographicData.phoneticGivenName()),
                           uuid, AppUtils.quotedValue(demographicData.phoneticFamilyName()),
                           uuid, AppUtils.quotedValue(demographicData.gender()),
                           uuid, AppUtils.quotedValue(demographicData.dob()),
                           uuid, AppUtils.quotedValue(demographicData.nupi()),
                           uuid, patientUID, score,
                           uuid);
   }
}
