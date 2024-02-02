package org.jembi.jempi.shared.models;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class CustomDemographicData {
   public final String givenName;
   public final String familyName;
   public final String gender;
   public final String dob;
   public final String nupi;
   public final String cccNumber;
   public final String docket;

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

   public final String getCccNumber() {
      return cccNumber;
   }

   public final String getDocket() {
      return docket;
   }

   public CustomDemographicData() {
      this(null, null, null, null, null, null, null);
   }

   public CustomDemographicData(
      final String givenName,
      final String familyName,
      final String gender,
      final String dob,
      final String nupi,
      final String cccNumber,
      final String docket) {
         this.givenName = givenName;
         this.familyName = familyName;
         this.gender = gender;
         this.dob = dob;
         this.nupi = nupi;
         this.cccNumber = cccNumber;
         this.docket = docket;
   }

   public CustomDemographicData clean() {
      return new CustomDemographicData(this.givenName.trim().toLowerCase().replaceAll("\\W", ""),
                                       this.familyName.trim().toLowerCase().replaceAll("\\W", ""),
                                       this.gender.trim().toLowerCase().replaceAll("\\W", ""),
                                       this.dob.trim().toLowerCase().replaceAll("\\W", ""),
                                       this.nupi.trim().toLowerCase().replaceAll("\\W", ""),
                                       this.cccNumber.trim().toLowerCase().replaceAll("\\W", ""),
                                       this.docket.trim().toLowerCase().replaceAll("\\W", ""));
   }

}
