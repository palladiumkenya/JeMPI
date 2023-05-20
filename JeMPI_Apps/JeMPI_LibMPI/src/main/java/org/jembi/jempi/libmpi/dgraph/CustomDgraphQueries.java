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
         query query_deterministic_golden_record_candidates($nupi: string) {
            all(func: eq(GoldenRecord.nupi, $nupi)) {
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

   static final String QUERY_MATCH_GOLDEN_RECORD_CANDIDATES_BY_DISTANCE =
         """
         query query_match_golden_record_candidates_by_distance($phonetic_given_name: string, $phonetic_family_name: string, $dob: string) {
            var(func: eq(GoldenRecord.phonetic_given_name, $phonetic_given_name)) {
               A as uid
            }
            var(func: eq(GoldenRecord.phonetic_family_name, $phonetic_family_name)) {
               B as uid
            }
            var(func: match(GoldenRecord.dob, $dob, 1)) {
               C as uid
            }
            all(func: uid(A,B,C)) @filter (uid(A) AND uid(B) AND uid(C)) {
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

   static final String QUERY_MATCH_GOLDEN_RECORD_CANDIDATES_BY_NUPI =
         """
         query query_match_golden_record_candidates_by_nupi($nupi: string) {
            all(func: match(GoldenRecord.nupi, $nupi, 1)) {
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
      if (StringUtils.isBlank(demographicData.nupi())) {
         return new DgraphGoldenRecords(List.of());
      }
      final Map<String, String> map = Map.of("$nupi", demographicData.nupi());
      return runGoldenRecordsQuery(QUERY_DETERMINISTIC_GOLDEN_RECORD_CANDIDATES, map);
   }

   static DgraphGoldenRecords queryMatchGoldenRecordCandidatesByDistance(final CustomDemographicData demographicData) {
      final var phoneticGivenName = demographicData.phoneticGivenName();
      final var phoneticFamilyName = demographicData.phoneticFamilyName();
      final var dob = demographicData.dob();
      final var phoneticGivenNameIsBlank = StringUtils.isBlank(phoneticGivenName);
      final var phoneticFamilyNameIsBlank = StringUtils.isBlank(phoneticFamilyName);
      final var dobIsBlank = StringUtils.isBlank(dob);
      if ((phoneticGivenNameIsBlank || phoneticFamilyNameIsBlank || dobIsBlank)) {
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
                             "$dob",
                             StringUtils.isNotBlank(dob)
                                   ? dob
                                   : DgraphQueries.EMPTY_FIELD_SENTINEL);
      return runGoldenRecordsQuery(QUERY_MATCH_GOLDEN_RECORD_CANDIDATES_BY_DISTANCE, map);
   }

   static DgraphGoldenRecords queryMatchGoldenRecordCandidatesByNupi(final CustomDemographicData demographicData) {
      if (StringUtils.isBlank(demographicData.nupi())) {
         return new DgraphGoldenRecords(List.of());
      }
      final Map<String, String> map = Map.of("$nupi", demographicData.nupi());
      return runGoldenRecordsQuery(QUERY_MATCH_GOLDEN_RECORD_CANDIDATES_BY_NUPI, map);
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
      updateCandidates(result, queryMatchGoldenRecordCandidatesByDistance(patient));
      updateCandidates(result, queryMatchGoldenRecordCandidatesByNupi(patient));
      return result;
   }

   private CustomDgraphQueries() {
   }
}
