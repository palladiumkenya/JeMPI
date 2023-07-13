package org.jembi.jempi.shared.models;

import com.fasterxml.jackson.annotation.JsonInclude;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class CustomDemographicData {
   public final String phoneticGivenName;
   public final String phoneticFamilyName;
   public final String gender;
   public final String dob;
   public final String nupi;
   public final String cccNumber;

   public final String getPhoneticGivenName() {
      return phoneticGivenName;
   }

   public final String getPhoneticFamilyName() {
      return phoneticFamilyName;
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

   public CustomDemographicData() {
      this(null, null, null, null, null, null);
   }

   public CustomDemographicData(
      final String phoneticGivenName,
      final String phoneticFamilyName,
      final String gender,
      final String dob,
      final String nupi,
      final String cccNumber) {
         this.phoneticGivenName = phoneticGivenName;
         this.phoneticFamilyName = phoneticFamilyName;
         this.gender = gender;
         this.dob = dob;
         this.nupi = nupi;
         this.cccNumber = cccNumber;
   }

   public CustomDemographicData clean() {
      return new CustomDemographicData(this.phoneticGivenName.toLowerCase().replaceAll("\\W", ""),
                                       this.phoneticFamilyName.toLowerCase().replaceAll("\\W", ""),
                                       this.gender.toLowerCase().replaceAll("\\W", ""),
                                       this.dob.toLowerCase().replaceAll("\\W", ""),
                                       this.nupi.toLowerCase().replaceAll("\\W", ""),
                                       this.cccNumber.toLowerCase().replaceAll("\\W", ""));
   }

}
