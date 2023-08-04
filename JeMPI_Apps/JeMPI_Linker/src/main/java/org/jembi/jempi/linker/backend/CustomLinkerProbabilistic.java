package org.jembi.jempi.linker.backend;


import org.jembi.jempi.shared.models.CustomDemographicData;
import org.jembi.jempi.shared.models.CustomMU;

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
