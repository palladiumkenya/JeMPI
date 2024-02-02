package org.jembi.jempi.libmpi.dgraph;

public final class CustomDgraphConstants {

   public static final String PREDICATE_GOLDEN_RECORD_AUX_DATE_CREATED = "GoldenRecord.aux_date_created";
   public static final String PREDICATE_GOLDEN_RECORD_AUX_AUTO_UPDATE_ENABLED = "GoldenRecord.aux_auto_update_enabled";
   public static final String PREDICATE_GOLDEN_RECORD_AUX_ID = "GoldenRecord.aux_id";
   public static final String PREDICATE_GOLDEN_RECORD_GIVEN_NAME = "GoldenRecord.given_name";
   public static final String PREDICATE_GOLDEN_RECORD_FAMILY_NAME = "GoldenRecord.family_name";
   public static final String PREDICATE_GOLDEN_RECORD_GENDER = "GoldenRecord.gender";
   public static final String PREDICATE_GOLDEN_RECORD_DOB = "GoldenRecord.dob";
   public static final String PREDICATE_GOLDEN_RECORD_NUPI = "GoldenRecord.nupi";
   public static final String PREDICATE_GOLDEN_RECORD_CCC_NUMBER = "GoldenRecord.ccc_number";
   public static final String PREDICATE_GOLDEN_RECORD_DOCKET = "GoldenRecord.docket";
   public static final String PREDICATE_GOLDEN_RECORD_INTERACTIONS = "GoldenRecord.interactions";
   public static final String PREDICATE_INTERACTION_AUX_DATE_CREATED = "Interaction.aux_date_created";
   public static final String PREDICATE_INTERACTION_AUX_ID = "Interaction.aux_id";
   public static final String PREDICATE_INTERACTION_PKV = "Interaction.pkv";
   public static final String PREDICATE_INTERACTION_AUX_DWH_ID = "Interaction.aux_dwh_id";
   public static final String PREDICATE_INTERACTION_GIVEN_NAME = "Interaction.given_name";
   public static final String PREDICATE_INTERACTION_FAMILY_NAME = "Interaction.family_name";
   public static final String PREDICATE_INTERACTION_GENDER = "Interaction.gender";
   public static final String PREDICATE_INTERACTION_DOB = "Interaction.dob";
   public static final String PREDICATE_INTERACTION_NUPI = "Interaction.nupi";
   public static final String PREDICATE_INTERACTION_CCC_NUMBER = "Interaction.ccc_number";
   public static final String PREDICATE_INTERACTION_DOCKET = "Interaction.docket";

   static final String GOLDEN_RECORD_FIELD_NAMES =
         """
         uid
         GoldenRecord.source_id {
            uid
            SourceId.facility
            SourceId.patient
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
         """;

   static final String EXPANDED_GOLDEN_RECORD_FIELD_NAMES =
         """
         uid
         GoldenRecord.source_id {
            uid
            SourceId.facility
            SourceId.patient
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
         GoldenRecord.interactions @facets(score) {
            uid
            Interaction.source_id {
               uid
               SourceId.facility
               SourceId.patient
            }
            Interaction.aux_date_created
            Interaction.aux_id
            Interaction.pkv
            Interaction.aux_dwh_id
            Interaction.given_name
            Interaction.family_name
            Interaction.gender
            Interaction.dob
            Interaction.nupi
            Interaction.ccc_number
            Interaction.docket
         }
         """;
   static final String INTERACTION_FIELD_NAMES =
         """
         uid
         Interaction.source_id {
            uid
            SourceId.facility
            SourceId.patient
         }
         Interaction.aux_date_created
         Interaction.aux_id
         Interaction.pkv
         Interaction.aux_dwh_id
         Interaction.given_name
         Interaction.family_name
         Interaction.gender
         Interaction.dob
         Interaction.nupi
         Interaction.ccc_number
         Interaction.docket
         """;

   static final String EXPANDED_INTERACTION_FIELD_NAMES =
         """
         uid
         Interaction.source_id {
            uid
            SourceId.facility
            SourceId.patient
         }
         Interaction.aux_date_created
         Interaction.aux_id
         Interaction.pkv
         Interaction.aux_dwh_id
         Interaction.given_name
         Interaction.family_name
         Interaction.gender
         Interaction.dob
         Interaction.nupi
         Interaction.ccc_number
         Interaction.docket
         ~GoldenRecord.interactions @facets(score) {
            uid
            GoldenRecord.source_id {
              uid
              SourceId.facility
              SourceId.patient
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
         """;

   static final String QUERY_GET_INTERACTION_BY_UID =
         """
         query interactionByUid($uid: string) {
            all(func: uid($uid)) {
               uid
               Interaction.source_id {
                 uid
                 SourceId.facility
                 SourceId.patient
               }
               Interaction.aux_date_created
               Interaction.aux_id
               Interaction.pkv
               Interaction.aux_dwh_id
               Interaction.given_name
               Interaction.family_name
               Interaction.gender
               Interaction.dob
               Interaction.nupi
               Interaction.ccc_number
               Interaction.docket
            }
         }
         """;

   static final String QUERY_GET_GOLDEN_RECORD_BY_UID =
         """
         query goldenRecordByUid($uid: string) {
            all(func: uid($uid)) {
               uid
               GoldenRecord.source_id {
                  uid
                  SourceId.facility
                  SourceId.patient
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

   static final String QUERY_GET_EXPANDED_INTERACTIONS =
         """
         query expandedInteraction() {
            all(func: uid(%s)) {
               uid
               Interaction.source_id {
                  uid
                  SourceId.facility
                  SourceId.patient
               }
               Interaction.aux_date_created
               Interaction.aux_id
               Interaction.pkv
               Interaction.aux_dwh_id
               Interaction.given_name
               Interaction.family_name
               Interaction.gender
               Interaction.dob
               Interaction.nupi
               Interaction.ccc_number
               Interaction.docket
               ~GoldenRecord.interactions @facets(score) {
                  uid
                  GoldenRecord.source_id {
                    uid
                    SourceId.facility
                    SourceId.patient
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
         }
         """;

   static final String QUERY_GET_GOLDEN_RECORDS =
         """
         query goldenRecord() {
            all(func: uid(%s)) {
               uid
               GoldenRecord.source_id {
                  uid
                  SourceId.facility
                  SourceId.patient
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

   static final String QUERY_GET_EXPANDED_GOLDEN_RECORDS =
         """
         query expandedGoldenRecord() {
            all(func: uid(%s), orderdesc: GoldenRecord.aux_date_created) {
               uid
               GoldenRecord.source_id {
                  uid
                  SourceId.facility
                  SourceId.patient
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
               GoldenRecord.interactions @facets(score) {
                  uid
                  Interaction.source_id {
                    uid
                    SourceId.facility
                    SourceId.patient
                  }
                  Interaction.aux_date_created
                  Interaction.aux_id
                  Interaction.pkv
                  Interaction.aux_dwh_id
                  Interaction.given_name
                  Interaction.family_name
                  Interaction.gender
                  Interaction.dob
                  Interaction.nupi
                  Interaction.ccc_number
                  Interaction.docket
               }
            }
         }
         """;

   static final String MUTATION_CREATE_SOURCE_ID_TYPE =
         """
         type SourceId {
            SourceId.facility
            SourceId.patient
         }
         """;
       
   static final String MUTATION_CREATE_SOURCE_ID_FIELDS =
         """
         SourceId.facility:                     string    @index(exact)                      .
         SourceId.patient:                      string    @index(exact)                      .
         """;
         
   static final String MUTATION_CREATE_GOLDEN_RECORD_TYPE =
         """

         type GoldenRecord {
            GoldenRecord.source_id:                 [SourceId]
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
            GoldenRecord.interactions:              [Interaction]
         }
         """;
           
   static final String MUTATION_CREATE_GOLDEN_RECORD_FIELDS =
         """
         GoldenRecord.source_id:                [uid]                                        .
         GoldenRecord.aux_date_created:         datetime                                     .
         GoldenRecord.aux_auto_update_enabled:  bool                                         .
         GoldenRecord.aux_id:                   string                                       .
         GoldenRecord.given_name:               string    @index(hash)                       .
         GoldenRecord.family_name:              string    @index(hash)                       .
         GoldenRecord.gender:                   string    @index(hash)                       .
         GoldenRecord.dob:                      string    @index(hash)                       .
         GoldenRecord.nupi:                     string    @index(hash)                       .
         GoldenRecord.ccc_number:               string    @index(hash)                       .
         GoldenRecord.docket:                   string    @index(hash)                       .
         GoldenRecord.interactions:             [uid]     @reverse                           .
         """;

   static final String MUTATION_CREATE_INTERACTION_TYPE =
         """

         type Interaction {
            Interaction.source_id:                     SourceId
            Interaction.aux_date_created
            Interaction.aux_id
            Interaction.pkv
            Interaction.aux_dwh_id
            Interaction.given_name
            Interaction.family_name
            Interaction.gender
            Interaction.dob
            Interaction.nupi
            Interaction.ccc_number
            Interaction.docket
         }
         """;

   static final String MUTATION_CREATE_INTERACTION_FIELDS =
         """
         Interaction.source_id:                    uid                                          .
         Interaction.aux_date_created:             datetime                                     .
         Interaction.aux_id:                       string                                       .
         Interaction.pkv:                          string                                       .
         Interaction.aux_dwh_id:                   string                                       .
         Interaction.given_name:                   string                                       .
         Interaction.family_name:                  string                                       .
         Interaction.gender:                       string                                       .
         Interaction.dob:                          string                                       .
         Interaction.nupi:                         string                                       .
         Interaction.ccc_number:                   string                                       .
         Interaction.docket:                       string                                       .
         """;

   private CustomDgraphConstants() {}

}
