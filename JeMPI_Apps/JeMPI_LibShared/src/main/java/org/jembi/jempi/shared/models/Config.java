package org.jembi.jempi.shared.models;

import java.util.List;

public record Config(
      List<Field> fields,
      List<SystemField> systemFields) {

   public record Field(
         String fieldName,
         String fieldType,
         String indexGoldenRecord,
         String fieldLabel,
         List<String> groups,
         List<String> scope,
         List<String> accessLevel) {
   }

   public record SystemField(
         String fieldName,
         String fieldType,
         String fieldLabel,
         List<String> groups,
         List<String> scope,
         List<String> accessLevel) {
   }

}

/*
  "rules": {
    "deterministic": {
      "QUERY_DETERMINISTIC_GOLDEN_RECORD_CANDIDATES": {
        "vars": [
          "given_name",
          "family_name",
          "phone_number",
          "national_id"
        ],
        "text": "eq(national_id) or (eq(given_name) and eq(family_name) and eq(phone_number))"
      }
    },
    "probabilistic": {
      "QUERY_MATCH_GOLDEN_RECORD_CANDIDATES_BY_DISTANCE": {
        "vars": [
          "given_name",
          "family_name",
          "city"
        ],
        "text": "match(given_name,3) and match(family_name,3) or match(given_name,3) and match(city,3) or match(family_name,3)
        and match(city,3)"
      },
      "QUERY_MATCH_GOLDEN_RECORD_CANDIDATES_BY_PHONE_NUMBER": {
        "vars": [
          "phone_number"
        ],
        "text": "match(phone_number,3)"
      },
      "QUERY_MATCH_GOLDEN_RECORD_CANDIDATES_BY_NATIONAL_ID": {
        "vars": [
          "national_id"
        ],
        "text": "match(national_id,3)"
      }
    }
  }
 */



