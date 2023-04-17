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
               GoldenRecord.patient_pkv
               GoldenRecord.site_code
               GoldenRecord.patient_pk
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
