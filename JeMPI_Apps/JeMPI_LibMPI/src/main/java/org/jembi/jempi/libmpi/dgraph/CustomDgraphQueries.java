package org.jembi.jempi.libmpi.dgraph;

import io.vavr.Function1;
import org.apache.commons.lang3.StringUtils;
import org.jembi.jempi.shared.models.CustomDemographicData;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.jembi.jempi.libmpi.dgraph.DgraphQueries.runGoldenRecordsQuery;

final class CustomDgraphQueries {

   static final List<Function1<CustomDemographicData, DgraphGoldenRecords>> DETERMINISTIC_FUNCTIONS =
      List.of(CustomDgraphQueries::queryDeterministicA,
              CustomDgraphQueries::queryDeterministicB);

   private static final String QUERY_DETERMINISTIC_A =
         """
         query query_deterministic_a($nupi: string) {
            all(func:type(GoldenRecord)) @filter(eq(GoldenRecord.nupi, $nupi)) {
               uid
               GoldenRecord.source_id {
                  uid
               }
               GoldenRecord.aux_date_created
               GoldenRecord.aux_auto_update_enabled
               GoldenRecord.aux_id
               GoldenRecord.given_name
               GoldenRecord.family_name
               GoldenRecord.gender
               GoldenRecord.dob
               GoldenRecord.nupi
            }
         }
         """;

   private static final String QUERY_DETERMINISTIC_B =
         """
         query query_deterministic_b($given_name: string, $family_name: string, $gender: string, $dob: string) {
            var(func:type(GoldenRecord)) @filter(eq(GoldenRecord.given_name, $given_name)) {
               A as uid
            }
            var(func:type(GoldenRecord)) @filter(eq(GoldenRecord.family_name, $family_name)) {
               B as uid
            }
            var(func:type(GoldenRecord)) @filter(eq(GoldenRecord.gender, $gender)) {
               C as uid
            }
            var(func:type(GoldenRecord)) @filter(eq(GoldenRecord.dob, $dob)) {
               D as uid
            }
            all(func:type(GoldenRecord)) @filter(uid(A) AND uid(B) AND uid(C) AND uid(D)) {
               uid
               GoldenRecord.source_id {
                  uid
               }
               GoldenRecord.aux_date_created
               GoldenRecord.aux_auto_update_enabled
               GoldenRecord.aux_id
               GoldenRecord.given_name
               GoldenRecord.family_name
               GoldenRecord.gender
               GoldenRecord.dob
               GoldenRecord.nupi
            }
         }
         """;

   private static final String QUERY_PROBABILISTIC =
         """
         query query_probabilistic($given_name: string, $family_name: string, $dob: string, $gender: string) {
            var(func:type(GoldenRecord)) @filter(eq(GoldenRecord.given_name, $given_name)) {
               A as uid
            }
            var(func:type(GoldenRecord)) @filter(eq(GoldenRecord.family_name, $family_name)) {
               B as uid
            }
            var(func:type(GoldenRecord)) @filter(eq(GoldenRecord.dob, $dob)) {
               C as uid
            }
            var(func:type(GoldenRecord)) @filter(eq(GoldenRecord.gender, $gender)) {
               D as uid
            }
            all(func:type(GoldenRecord)) @filter((uid(A) AND uid(B)) OR ((uid(A) OR uid(B)) AND uid(C) AND uid(D))) {
               uid
               GoldenRecord.source_id {
                  uid
               }
               GoldenRecord.aux_date_created
               GoldenRecord.aux_auto_update_enabled
               GoldenRecord.aux_id
               GoldenRecord.given_name
               GoldenRecord.family_name
               GoldenRecord.gender
               GoldenRecord.dob
               GoldenRecord.nupi
            }
         }
         """;


   private static DgraphGoldenRecords queryDeterministicA(final CustomDemographicData demographicData) {
      if (StringUtils.isBlank(demographicData.nupi)) {
         return new DgraphGoldenRecords(List.of());
      }
      final Map<String, String> map = Map.of("$nupi", demographicData.nupi);
      return runGoldenRecordsQuery(QUERY_DETERMINISTIC_A, map);
   }

   private static DgraphGoldenRecords queryDeterministicB(final CustomDemographicData demographicData) {
      final var givenName = demographicData.givenName;
      final var familyName = demographicData.familyName;
      final var gender = demographicData.gender;
      final var dob = demographicData.dob;
      final var givenNameIsBlank = StringUtils.isBlank(givenName);
      final var familyNameIsBlank = StringUtils.isBlank(familyName);
      final var genderIsBlank = StringUtils.isBlank(gender);
      final var dobIsBlank = StringUtils.isBlank(dob);
      if ((givenNameIsBlank || familyNameIsBlank || genderIsBlank || dobIsBlank)) {
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
                                   : DgraphQueries.EMPTY_FIELD_SENTINEL);
      return runGoldenRecordsQuery(QUERY_DETERMINISTIC_B, map);
   }

   private static DgraphGoldenRecords queryProbabilistic(final CustomDemographicData demographicData) {
      final var givenName = demographicData.givenName;
      final var familyName = demographicData.familyName;
      final var dob = demographicData.dob;
      final var gender = demographicData.gender;
      final var givenNameIsBlank = StringUtils.isBlank(givenName);
      final var familyNameIsBlank = StringUtils.isBlank(familyName);
      final var dobIsBlank = StringUtils.isBlank(dob);
      final var genderIsBlank = StringUtils.isBlank(gender);
      if (((givenNameIsBlank || familyNameIsBlank) && ((givenNameIsBlank && familyNameIsBlank) || dobIsBlank || genderIsBlank))) {
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
                                   : DgraphQueries.EMPTY_FIELD_SENTINEL,
                             "$gender",
                             StringUtils.isNotBlank(gender)
                                   ? gender
                                   : DgraphQueries.EMPTY_FIELD_SENTINEL);
      return runGoldenRecordsQuery(QUERY_PROBABILISTIC, map);
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
      final CustomDemographicData interaction) {
      var result = DgraphQueries.deterministicFilter(interaction);
      if (!result.isEmpty()) {
         return result;
      }
      result = new LinkedList<>();
      updateCandidates(result, queryProbabilistic(interaction));
      return result;
   }

   private CustomDgraphQueries() {
   }
}
