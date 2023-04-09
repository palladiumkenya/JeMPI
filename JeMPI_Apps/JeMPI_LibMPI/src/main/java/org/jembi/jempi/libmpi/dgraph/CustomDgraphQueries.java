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
         query query_deterministic_golden_record_candidates($given_name: string, $family_name: string, $gender: string, $dob: string, $national_id: string) {
            var(func: eq(GoldenRecord.given_name, $given_name)) {
               A as uid
            }
            var(func: eq(GoldenRecord.family_name, $family_name)) {
               B as uid
            }
            var(func: eq(GoldenRecord.gender, $gender)) {
               C as uid
            }
            var(func: eq(GoldenRecord.dob, $dob)) {
               D as uid
            }
            var(func: eq(GoldenRecord.national_id, $national_id)) {
               E as uid
            }
            all(func: uid(D,B,E,A,C)) @filter (uid(E) OR (uid(A) AND uid(B) AND uid(C) AND uid(D))) {
               uid
               GoldenRecord.source_id {
                  uid
               }
               GoldenRecord.aux_id
               GoldenRecord.given_name
               GoldenRecord.family_name
               GoldenRecord.gender
               GoldenRecord.dob
               GoldenRecord.national_id
            }
         }
         """;

   static final String QUERY_MATCH_GOLDEN_RECORD_CANDIDATES_BY_DISTANCE =
         """
         query query_match_golden_record_candidates_by_distance($given_name: string, $family_name: string, $dob: string) {
            var(func: match(GoldenRecord.given_name, $given_name, 3)) {
               A as uid
            }
            var(func: match(GoldenRecord.family_name, $family_name, 3)) {
               B as uid
            }
            var(func: match(GoldenRecord.dob, $dob, 3)) {
               C as uid
            }
            all(func: uid(A,B,C)) @filter ((uid(A) AND uid(B)) OR (uid(A) AND uid(C)) OR (uid(B) AND uid(C))) {
               uid
               GoldenRecord.source_id {
                  uid
               }
               GoldenRecord.aux_id
               GoldenRecord.given_name
               GoldenRecord.family_name
               GoldenRecord.gender
               GoldenRecord.dob
               GoldenRecord.national_id
            }
         }
         """;

   static final String QUERY_MATCH_GOLDEN_RECORD_CANDIDATES_BY_NATIONAL_ID =
         """
         query query_match_golden_record_candidates_by_national_id($national_id: string) {
            all(func: match(GoldenRecord.national_id, $national_id, 3)) {
               uid
               GoldenRecord.source_id {
                  uid
               }
               GoldenRecord.aux_id
               GoldenRecord.given_name
               GoldenRecord.family_name
               GoldenRecord.gender
               GoldenRecord.dob
               GoldenRecord.national_id
            }
         }
         """;


   static DgraphGoldenRecords queryDeterministicGoldenRecordCandidates(final CustomDemographicData demographicData) {
      final var givenName = demographicData.givenName();
      final var familyName = demographicData.familyName();
      final var gender = demographicData.gender();
      final var dob = demographicData.dob();
      final var nationalId = demographicData.nationalId();
      final var givenNameIsBlank = StringUtils.isBlank(givenName);
      final var familyNameIsBlank = StringUtils.isBlank(familyName);
      final var genderIsBlank = StringUtils.isBlank(gender);
      final var dobIsBlank = StringUtils.isBlank(dob);
      final var nationalIdIsBlank = StringUtils.isBlank(nationalId);
      if ((nationalIdIsBlank && (givenNameIsBlank || familyNameIsBlank || genderIsBlank || dobIsBlank))) {
         return new DgraphGoldenRecords(List.of());
      }
      final var map = Map.of("$given_name",
                             StringUtils.isNotBlank(givenName)
                                   ? givenName
                                   : DgraphQueries.EMPTY_FIELD_SENTINEL,
                             "$family_name",
                             StringUtils.isNotBlank(familyName)
                                   ? familyName
                                   : DgraphQueries.EMPTY_FIELD_SENTINEL,
                             "$gender",
                             StringUtils.isNotBlank(gender)
                                   ? gender
                                   : DgraphQueries.EMPTY_FIELD_SENTINEL,
                             "$dob",
                             StringUtils.isNotBlank(dob)
                                   ? dob
                                   : DgraphQueries.EMPTY_FIELD_SENTINEL,
                             "$national_id",
                             StringUtils.isNotBlank(nationalId)
                                   ? nationalId
                                   : DgraphQueries.EMPTY_FIELD_SENTINEL);
      return runGoldenRecordsQuery(QUERY_DETERMINISTIC_GOLDEN_RECORD_CANDIDATES, map);
   }

   static DgraphGoldenRecords queryMatchGoldenRecordCandidatesByDistance(final CustomDemographicData demographicData) {
      final var givenName = demographicData.givenName();
      final var familyName = demographicData.familyName();
      final var dob = demographicData.dob();
      final var givenNameIsBlank = StringUtils.isBlank(givenName);
      final var familyNameIsBlank = StringUtils.isBlank(familyName);
      final var dobIsBlank = StringUtils.isBlank(dob);
      if (((givenNameIsBlank || familyNameIsBlank) && (givenNameIsBlank || dobIsBlank) && (familyNameIsBlank || dobIsBlank))) {
         return new DgraphGoldenRecords(List.of());
      }
      final var map = Map.of("$given_name",
                             StringUtils.isNotBlank(givenName)
                                   ? givenName
                                   : DgraphQueries.EMPTY_FIELD_SENTINEL,
                             "$family_name",
                             StringUtils.isNotBlank(familyName)
                                   ? familyName
                                   : DgraphQueries.EMPTY_FIELD_SENTINEL,
                             "$dob",
                             StringUtils.isNotBlank(dob)
                                   ? dob
                                   : DgraphQueries.EMPTY_FIELD_SENTINEL);
      return runGoldenRecordsQuery(QUERY_MATCH_GOLDEN_RECORD_CANDIDATES_BY_DISTANCE, map);
   }

   static DgraphGoldenRecords queryMatchGoldenRecordCandidatesByNationalId(final CustomDemographicData demographicData) {
      if (StringUtils.isBlank(demographicData.nationalId())) {
         return new DgraphGoldenRecords(List.of());
      }
      final Map<String, String> map = Map.of("$national_id", demographicData.nationalId());
      return runGoldenRecordsQuery(QUERY_MATCH_GOLDEN_RECORD_CANDIDATES_BY_NATIONAL_ID, map);
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
      updateCandidates(result, queryMatchGoldenRecordCandidatesByNationalId(patient));
      return result;
   }

   private CustomDgraphQueries() {
   }
}
