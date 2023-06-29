package org.jembi.jempi.shared.models;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public record CustomMU(Probability phoneticGivenName,
                       Probability phoneticFamilyName,
                       Probability gender,
                       Probability dob,
                       Probability nupi) {

   public CustomMU(final double[] mHat, final double[] uHat) {
      this(new CustomMU.Probability((float) mHat[0], (float) uHat[0]),
           new CustomMU.Probability((float) mHat[1], (float) uHat[1]),
           new CustomMU.Probability((float) mHat[2], (float) uHat[2]),
           new CustomMU.Probability((float) mHat[3], (float) uHat[3]),
           new CustomMU.Probability((float) mHat[4], (float) uHat[4]));
   }

   public record Probability(float m, float u) {
   }

}
