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
      List.of(CustomDgraphQueries::queryDeterministicA);

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


   private static DgraphGoldenRecords queryDeterministicA(final CustomDemographicData demographicData) {
      if (StringUtils.isBlank(demographicData.nupi)) {
         return new DgraphGoldenRecords(List.of());
      }
      final Map<String, String> map = Map.of("$nupi", demographicData.nupi);
      return runGoldenRecordsQuery(QUERY_DETERMINISTIC_A, map);
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
      return result;
   }

   private CustomDgraphQueries() {
   }
}
