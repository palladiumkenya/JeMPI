package org.jembi.jempi.libmpi.dgraph;

import org.apache.commons.lang3.StringUtils;
import org.jembi.jempi.shared.models.CustomDemographicData;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.jembi.jempi.libmpi.dgraph.DgraphQueries.runGoldenRecordsQuery;

final class CustomDgraphQueries {

   static final String QUERY_DETERMINISTIC_GOLDEN_RECORD_CANDIDATES =
         """
         query query_deterministic_golden_record_candidates($phonetic_given_name: string, $phonetic_family_name: string, $gender: string, $dob: string, $nupi: string) {
            var(func: eq(GoldenRecord.phonetic_given_name, $phonetic_given_name)) {
               A as uid
            }
            var(func: eq(GoldenRecord.phonetic_family_name, $phonetic_family_name)) {
               B as uid
            }
            var(func: eq(GoldenRecord.gender, $gender)) {
               C as uid
            }
            var(func: eq(GoldenRecord.dob, $dob)) {
               D as uid
            }
            var(func: eq(GoldenRecord.nupi, $nupi)) {
               E as uid
            }
            all(func: uid(D,E,A,B,C)) @filter (uid(E) OR (uid(A) AND uid(B) AND uid(C) AND uid(D))) {
               uid
               GoldenRecord.source_id {
                  uid
               }
               GoldenRecord.aux_id
               GoldenRecord.aux_dwh_id
               GoldenRecord.phonetic_given_name
               GoldenRecord.phonetic_family_name
               GoldenRecord.gender
               GoldenRecord.dob
               GoldenRecord.nupi
            }
         }
         """;


   static DgraphGoldenRecords queryDeterministicGoldenRecordCandidates(final CustomDemographicData demographicData) {
      final var phoneticGivenName = demographicData.phoneticGivenName();
      final var phoneticFamilyName = demographicData.phoneticFamilyName();
      final var gender = demographicData.gender();
      final var dob = demographicData.dob();
      final var nupi = demographicData.nupi();
      final var phoneticGivenNameIsBlank = StringUtils.isBlank(phoneticGivenName);
      final var phoneticFamilyNameIsBlank = StringUtils.isBlank(phoneticFamilyName);
      final var genderIsBlank = StringUtils.isBlank(gender);
      final var dobIsBlank = StringUtils.isBlank(dob);
      final var nupiIsBlank = StringUtils.isBlank(nupi);
      if ((nupiIsBlank && (phoneticGivenNameIsBlank || phoneticFamilyNameIsBlank || genderIsBlank || dobIsBlank))) {
         return new DgraphGoldenRecords(List.of());
      }
      final var map = Map.of("$phonetic_given_name",
                             StringUtils.isNotBlank(phoneticGivenName)
                                   ? phoneticGivenName
                                   : DgraphQueries.EMPTY_FIELD_SENTINEL,
                             "$phonetic_family_name",
                             StringUtils.isNotBlank(phoneticFamilyName)
                                   ? phoneticFamilyName
                                   : DgraphQueries.EMPTY_FIELD_SENTINEL,
                             "$gender",
                             StringUtils.isNotBlank(gender)
                                   ? gender
                                   : DgraphQueries.EMPTY_FIELD_SENTINEL,
                             "$dob",
                             StringUtils.isNotBlank(dob)
                                   ? dob
                                   : DgraphQueries.EMPTY_FIELD_SENTINEL,
                             "$nupi",
                             StringUtils.isNotBlank(nupi)
                                   ? nupi
                                   : DgraphQueries.EMPTY_FIELD_SENTINEL);
      return runGoldenRecordsQuery(QUERY_DETERMINISTIC_GOLDEN_RECORD_CANDIDATES, map);
   }

   private static void updateCandidates(
         final List<CustomDgraphGoldenRecord> goldenRecords,
         final DgraphGoldenRecords block) {
      final var candidates = block.all();
      if (!candidates.isEmpty()) {
         candidates.forEach(candidate -> {
            var found = false;
            for (CustomDgraphGoldenRecord goldenRecord : goldenRecords) {
               if (candidate.goldenId().equals(goldenRecord.goldenId())) {
                  found = true;
                  break;
               }
            }
            if (!found) {
               goldenRecords.add(candidate);
            }
         });
      }
   }

   static List<CustomDgraphGoldenRecord> getCandidates(
         final CustomDemographicData patient,
         final boolean applyDeterministicFilter) {

      if (applyDeterministicFilter) {
         final var result = DgraphQueries.deterministicFilter(patient);
         if (!result.isEmpty()) {
            return result;
         }
      }
      var result = new LinkedList<CustomDgraphGoldenRecord>();
      return result;
   }

   private CustomDgraphQueries() {
   }
}
