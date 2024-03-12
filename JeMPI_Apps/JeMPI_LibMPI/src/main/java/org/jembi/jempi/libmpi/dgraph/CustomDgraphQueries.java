package org.jembi.jempi.libmpi.dgraph;

import io.vavr.Function1;
import org.apache.commons.lang3.StringUtils;
import org.jembi.jempi.shared.models.CustomDemographicData;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.jembi.jempi.libmpi.dgraph.DgraphQueries.runGoldenRecordsQuery;

final class CustomDgraphQueries {

   static final List<Function1<CustomDemographicData, DgraphGoldenRecords>> DETERMINISTIC_LINK_FUNCTIONS =
      List.of(CustomDgraphQueries::queryDeterministicA);

   static final List<Function1<CustomDemographicData, DgraphGoldenRecords>> DETERMINISTIC_MATCH_FUNCTIONS =
      List.of(CustomDgraphQueries::queryMatchDeterministicA);

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
               GoldenRecord.ccc_number
               GoldenRecord.docket
            }
         }
         """;

   private static final String QUERY_MATCH_DETERMINISTIC_A =
         """
         query query_match_deterministic_a($ccc_number: string) {
            all(func:type(GoldenRecord)) @filter(eq(GoldenRecord.ccc_number, $ccc_number)) {
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
               GoldenRecord.ccc_number
               GoldenRecord.docket
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

   private static void mergeCandidates(
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

   static List<CustomDgraphGoldenRecord> findLinkCandidates(
      final CustomDemographicData interaction) {
      var result = DgraphQueries.deterministicFilter(DETERMINISTIC_LINK_FUNCTIONS, interaction);
      if (!result.isEmpty()) {
         return result;
      }
      result = new LinkedList<>();
      return result;
   }

   private static DgraphGoldenRecords queryMatchDeterministicA(final CustomDemographicData demographicData) {
      if (StringUtils.isBlank(demographicData.cccNumber)) {
         return new DgraphGoldenRecords(List.of());
      }
      final Map<String, String> map = Map.of("$ccc_number", demographicData.cccNumber);
      return runGoldenRecordsQuery(QUERY_MATCH_DETERMINISTIC_A, map);
   }

   static List<CustomDgraphGoldenRecord> findMatchCandidates(
      final CustomDemographicData interaction) {
      var result = DgraphQueries.deterministicFilter(DETERMINISTIC_MATCH_FUNCTIONS, interaction);
      if (!result.isEmpty()) {
         return result;
      }
      result = new LinkedList<>();
      return result;
   }

   private CustomDgraphQueries() {
   }

}
