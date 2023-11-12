package org.jembi.jempi.shared.models;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class CustomDemographicData {
   public final String givenName;
   public final String familyName;
   public final String gender;
   public final String dob;
   public final String nupi;

   public final String getGivenName() {
      return givenName;
   }

   public final String getFamilyName() {
      return familyName;
   }

   public final String getGender() {
      return gender;
   }

   public final String getDob() {
      return dob;
   }

   public final String getNupi() {
      return nupi;
   }

   public CustomDemographicData() {
      this(null, null, null, null, null);
   }

   public CustomDemographicData(
      final String givenName,
      final String familyName,
      final String gender,
      final String dob,
      final String nupi) {
         this.givenName = givenName;
         this.familyName = familyName;
         this.gender = gender;
         this.dob = dob;
         this.nupi = nupi;
   }

   public CustomDemographicData clean() {
      return new CustomDemographicData(this.givenName.toLowerCase().replaceAll("\\W", ""),
                                       this.familyName.toLowerCase().replaceAll("\\W", ""),
                                       this.gender.toLowerCase().replaceAll("\\W", ""),
                                       this.dob.toLowerCase().replaceAll("\\W", ""),
                                       this.nupi.toLowerCase().replaceAll("\\W", ""));
   }

}
