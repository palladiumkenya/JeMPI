package org.jembi.jempi.libmpi.dgraph;

public final class CustomDgraphConstants {

   public static final String PREDICATE_GOLDEN_RECORD_AUX_ID = "GoldenRecord.aux_id";
   public static final String PREDICATE_GOLDEN_RECORD_AUX_DWH_ID = "GoldenRecord.aux_dwh_id";
   public static final String PREDICATE_GOLDEN_RECORD_PHONETIC_GIVEN_NAME = "GoldenRecord.phonetic_given_name";
   public static final String PREDICATE_GOLDEN_RECORD_PHONETIC_FAMILY_NAME = "GoldenRecord.phonetic_family_name";
   public static final String PREDICATE_GOLDEN_RECORD_GENDER = "GoldenRecord.gender";
   public static final String PREDICATE_GOLDEN_RECORD_DOB = "GoldenRecord.dob";
   public static final String PREDICATE_GOLDEN_RECORD_NUPI = "GoldenRecord.nupi";
   public static final String PREDICATE_GOLDEN_RECORD_PATIENTS = "GoldenRecord.patients";
   public static final String PREDICATE_PATIENT_RECORDAUX_ID = "PatientRecord.aux_id";
   public static final String PREDICATE_PATIENT_RECORDAUX_DWH_ID = "PatientRecord.aux_dwh_id";
   public static final String PREDICATE_PATIENT_RECORDPHONETIC_GIVEN_NAME = "PatientRecord.phonetic_given_name";
   public static final String PREDICATE_PATIENT_RECORDPHONETIC_FAMILY_NAME = "PatientRecord.phonetic_family_name";
   public static final String PREDICATE_PATIENT_RECORDGENDER = "PatientRecord.gender";
   public static final String PREDICATE_PATIENT_RECORDDOB = "PatientRecord.dob";
   public static final String PREDICATE_PATIENT_RECORDNUPI = "PatientRecord.nupi";

   static final String GOLDEN_RECORD_FIELD_NAMES =
         """
         uid
         GoldenRecord.source_id {
            uid
            SourceId.facility
            SourceId.patient
         }
         GoldenRecord.aux_id
         GoldenRecord.aux_dwh_id
         GoldenRecord.phonetic_given_name
         GoldenRecord.phonetic_family_name
         GoldenRecord.gender
         GoldenRecord.dob
         GoldenRecord.nupi
         """;

   static final String EXPANDED_GOLDEN_RECORD_FIELD_NAMES =
         """
         uid
         GoldenRecord.source_id {
            uid
            SourceId.facility
            SourceId.patient
         }
         GoldenRecord.aux_id
         GoldenRecord.aux_dwh_id
         GoldenRecord.phonetic_given_name
         GoldenRecord.phonetic_family_name
         GoldenRecord.gender
         GoldenRecord.dob
         GoldenRecord.nupi
         GoldenRecord.patients @facets(score) {
            uid
            PatientRecord.source_id {
               uid
               SourceId.facility
               SourceId.patient
            }
            PatientRecord.aux_id
            PatientRecord.aux_dwh_id
            PatientRecord.phonetic_given_name
            PatientRecord.phonetic_family_name
            PatientRecord.gender
            PatientRecord.dob
            PatientRecord.nupi
         }
         """;
   static final String PATIENT_RECORD_FIELD_NAMES =
         """
         uid
         PatientRecord.source_id {
            uid
            SourceId.facility
            SourceId.patient
         }
         PatientRecord.aux_id
         PatientRecord.aux_dwh_id
         PatientRecord.phonetic_given_name
         PatientRecord.phonetic_family_name
         PatientRecord.gender
         PatientRecord.dob
         PatientRecord.nupi
         """;
   static final String EXPANDED_PATIENT_RECORD_FIELD_NAMES =
         """
         uid
         PatientRecord.source_id {
            uid
            SourceId.facility
            SourceId.patient
         }
         PatientRecord.aux_id
         PatientRecord.aux_dwh_id
         PatientRecord.phonetic_given_name
         PatientRecord.phonetic_family_name
         PatientRecord.gender
         PatientRecord.dob
         PatientRecord.nupi
         ~GoldenRecord.patients @facets(score) {
            uid
            GoldenRecord.source_id {
              uid
              SourceId.facility
              SourceId.patient
            }
            GoldenRecord.aux_id
            GoldenRecord.aux_dwh_id
            GoldenRecord.phonetic_given_name
            GoldenRecord.phonetic_family_name
            GoldenRecord.gender
            GoldenRecord.dob
            GoldenRecord.nupi
         }
         """;

   static final String QUERY_GET_PATIENT_BY_UID =
         """
         query patientByUid($uid: string) {
            all(func: uid($uid)) {
               uid
               PatientRecord.source_id {
                 uid
                 SourceId.facility
                 SourceId.patient
               }
               PatientRecord.aux_id
               PatientRecord.aux_dwh_id
               PatientRecord.phonetic_given_name
               PatientRecord.phonetic_family_name
               PatientRecord.gender
               PatientRecord.dob
               PatientRecord.nupi
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
               GoldenRecord.aux_id
               GoldenRecord.aux_dwh_id
               GoldenRecord.phonetic_given_name
               GoldenRecord.phonetic_family_name
               GoldenRecord.gender
               GoldenRecord.dob
               GoldenRecord.nupi
            }
         }
         """;

   static final String QUERY_GET_EXPANDED_PATIENTS =
         """
         query expandedPatient() {
            all(func: uid(%s)) {
               uid
               PatientRecord.source_id {
                  uid
                  SourceId.facility
                  SourceId.patient
               }
               PatientRecord.aux_id
               PatientRecord.aux_dwh_id
               PatientRecord.phonetic_given_name
               PatientRecord.phonetic_family_name
               PatientRecord.gender
               PatientRecord.dob
               PatientRecord.nupi
               ~GoldenRecord.patients @facets(score) {
                  uid
                  GoldenRecord.source_id {
                    uid
                    SourceId.facility
                    SourceId.patient
                  }
                  GoldenRecord.aux_id
                  GoldenRecord.aux_dwh_id
                  GoldenRecord.phonetic_given_name
                  GoldenRecord.phonetic_family_name
                  GoldenRecord.gender
                  GoldenRecord.dob
                  GoldenRecord.nupi
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
               GoldenRecord.aux_id
               GoldenRecord.aux_dwh_id
               GoldenRecord.phonetic_given_name
               GoldenRecord.phonetic_family_name
               GoldenRecord.gender
               GoldenRecord.dob
               GoldenRecord.nupi
            }
         }
         """;

   static final String QUERY_GET_EXPANDED_GOLDEN_RECORDS =
         """
         query expandedGoldenRecord() {
            all(func: uid(%s)) {
               uid
               GoldenRecord.source_id {
                  uid
                  SourceId.facility
                  SourceId.patient
               }
               GoldenRecord.aux_id
               GoldenRecord.aux_dwh_id
               GoldenRecord.phonetic_given_name
               GoldenRecord.phonetic_family_name
               GoldenRecord.gender
               GoldenRecord.dob
               GoldenRecord.nupi
               GoldenRecord.patients @facets(score) {
                  uid
                  PatientRecord.source_id {
                    uid
                    SourceId.facility
                    SourceId.patient
                  }
                  PatientRecord.aux_id
                  PatientRecord.aux_dwh_id
                  PatientRecord.phonetic_given_name
                  PatientRecord.phonetic_family_name
                  PatientRecord.gender
                  PatientRecord.dob
                  PatientRecord.nupi
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
            GoldenRecord.aux_id
            GoldenRecord.aux_dwh_id
            GoldenRecord.phonetic_given_name
            GoldenRecord.phonetic_family_name
            GoldenRecord.gender
            GoldenRecord.dob
            GoldenRecord.nupi
            GoldenRecord.patients:                  [PatientRecord]
         }
         """;
         
   static final String MUTATION_CREATE_GOLDEN_RECORD_FIELDS =
         """
         GoldenRecord.source_id:                [uid]                                        .
         GoldenRecord.aux_id:                   string    @index(exact,trigram)              .
         GoldenRecord.aux_dwh_id:               string    @index(exact,trigram)              .
         GoldenRecord.phonetic_given_name:      string    @index(exact)                      .
         GoldenRecord.phonetic_family_name:     string    @index(exact)                      .
         GoldenRecord.gender:                   string    @index(exact,trigram)              .
         GoldenRecord.dob:                      string    @index(exact,trigram)              .
         GoldenRecord.nupi:                     string    @index(exact,trigram)              .
         GoldenRecord.patients:                 [uid]     @reverse                           .
         """;

   static final String MUTATION_CREATE_PATIENT_TYPE =
         """

         type PatientRecord {
            PatientRecord.source_id:                     SourceId
            PatientRecord.aux_id
            PatientRecord.aux_dwh_id
            PatientRecord.phonetic_given_name
            PatientRecord.phonetic_family_name
            PatientRecord.gender
            PatientRecord.dob
            PatientRecord.nupi
         }
         """;

   static final String MUTATION_CREATE_PATIENT_FIELDS =
         """
         PatientRecord.source_id:                    uid                                          .
         PatientRecord.aux_id:                       string                                       .
         PatientRecord.aux_dwh_id:                   string                                       .
         PatientRecord.phonetic_given_name:          string                                       .
         PatientRecord.phonetic_family_name:         string                                       .
         PatientRecord.gender:                       string                                       .
         PatientRecord.dob:                          string                                       .
         PatientRecord.nupi:                         string                                       .
         """;

   private CustomDgraphConstants() {}

}
