package org.jembi.jempi.linker.backend;


import org.jembi.jempi.shared.models.CustomDemographicData;
           import org.jembi.jempi.shared.models.CustomMU;

import static org.jembi.jempi.linker.backend.LinkerProbabilistic.EXACT_SIMILARITY;
import static org.jembi.jempi.linker.backend.LinkerProbabilistic.JACCARD_SIMILARITY;
import static org.jembi.jempi.linker.backend.LinkerProbabilistic.JARO_SIMILARITY;
import static org.jembi.jempi.linker.backend.LinkerProbabilistic.JARO_WINKLER_SIMILARITY;

final class CustomLinkerProbabilistic {

  private CustomLinkerProbabilistic() {
  }

  public static float probabilisticScore(final CustomDemographicData goldenRecord,
                                         final CustomDemographicData interaction) {
    return 0.0F;
  }

  public static void updateMU(final CustomMU mu) {
  }

  public static void checkUpdatedMU() {
  }

  static CustomMU getMU() {
    return new CustomMU(null);
  }

}
