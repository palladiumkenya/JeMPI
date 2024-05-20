package org.jembi.jempi.async_receiver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jembi.jempi.AppConfig;
import org.jembi.jempi.shared.kafka.MyKafkaProducer;
import org.jembi.jempi.shared.models.*;
import org.postgresql.util.PGobject;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;


final class DWH {
   private MyKafkaProducer<String, InteractionEnvelop> interactionEnvelopProducer;
   private static final String SQL_INSERT = """
                                            INSERT INTO mpi_matching_output(gender,dob,nupi,ccc_number,site_code,patient_pk,pkv,docket)
                                            VALUES (?,?,?,?,?,?,?,?)
                                            """;


   private static final String SQL_UPDATE = """
                                            UPDATE mpi_matching_output
                                            SET golden_id = ?, encounter_id = ?, phonetic_given_name = ?, phonetic_family_name = ?
                                            WHERE dwh_id = ?
                                            """;
   private static final String SQL_PATIENT_LIST = """
           with
               ct_patient_source
               as
               (
                   select
                       distinct
                       patients.PatientID as CCCNumber,
                       patients.PatientPK,
                       patients.SiteCode,
                       case
           			when Gender = 'F' then 'Female'
           			when Gender = 'M' then 'Male'
           			when Gender = '' then NULL
           			else Gender
           			end as Gender,
                       cast(DOB as date) as DOB,
                       case
           			when NUPI = '' then NULL
           			else  NUPI
           			end as NUPI,
                       Pkv,
                       'C&T' as Docket
                   from
                       ODS.dbo.CT_Patient as patients
               )
                      
               ,
               hts_patient_source
               as
               (
                   select
                       distinct
                       PatientPK,
                       SiteCode,
                       case
           			when Gender = 'F' then 'Female'
           			when Gender = 'M' then 'Male'
           			when Gender = '' then NULL
           			else Gender
           			end as Gender,
                       cast(DOB as date) as DOB,
                       case
           			when NUPI = '' then NULL
           			else  NUPI
           			end as NUPI,
                       PKV,
                       'HTS' as Docket
                   from ODS.dbo.HTS_clients as clients
               )
                      
               ,
               prep_patient_source
               as
               (
                   select
                       distinct
                       PatientPk,
                       PrepNumber,
                       SiteCode,
                       Sex AS Gender,
                       cast(DateofBirth as date) as DOB,
                       'PrEP' as Docket
                   from ODS.dbo.PrEP_Patient
               )
                      
              ,
               mnch_patient_source
               as
               (
                   select
                       distinct
                       PatientPk,
                       SiteCode,
                       Gender,
                       cast(DOB as date) as DOB,
                       case
           		when NUPI = '' then NULL
           		else  NUPI
           		end as NUPI,
                       Pkv,
                       'MNCH' as Docket
                   from ODS.dbo.MNCH_Patient
               )
                      
           	,
               combined_data_ct_hts
               as
               (
                   select
                       coalesce(ct_patient_source.PatientPK, hts_patient_source.PatientPK) as PatientPK,
                       coalesce(ct_patient_source.SiteCode, hts_patient_source.SiteCode) as SiteCode,
                       ct_patient_source.CCCNumber,
                       coalesce(ct_patient_source.NUPI, hts_patient_source.NUPI) as NUPI,
                       coalesce(ct_patient_source.DOB, hts_patient_source.DOB) as DOB,
                       coalesce(ct_patient_source.Gender, hts_patient_source.Gender) as Gender,
                       coalesce(ct_patient_source.PKV, hts_patient_source.PKV) as PKV,
                       iif(ct_patient_source.Docket is not null and hts_patient_source.Docket is not null,
           	CONCAT_WS('|', ct_patient_source.Docket, hts_patient_source.Docket),
           	coalesce(ct_patient_source.Docket, hts_patient_source.Docket)) as Docket
                   from ct_patient_source full join hts_patient_source on  hts_patient_source.PatientPK = ct_patient_source.PatientPK
                           and ct_patient_source.SiteCode = hts_patient_source.SiteCode
               ),
                      
               combined_data_ct_hts_prep
               as
               (
                   select
                       coalesce(combined_data_ct_hts.PatientPK, prep_patient_source.PatientPK) as PatientPK,
                       coalesce(combined_data_ct_hts.SiteCode, prep_patient_source.SiteCode) as SiteCode,
                       combined_data_ct_hts.CCCNumber,
                       combined_data_ct_hts.NUPI as NUPI,
                       coalesce(combined_data_ct_hts.DOB, prep_patient_source.DOB) as DOB,
                       coalesce(combined_data_ct_hts.Gender, prep_patient_source.Gender) as Gender,
                       combined_data_ct_hts.PKV,
                       iif(combined_data_ct_hts.Docket is not null and prep_patient_source.Docket is not null,
           	CONCAT_WS('|', combined_data_ct_hts.Docket, prep_patient_source.Docket),
           	coalesce(combined_data_ct_hts.Docket, prep_patient_source.Docket)) as Docket
                   from combined_data_ct_hts
                       full join prep_patient_source on combined_data_ct_hts.PatientPK = prep_patient_source.PatientPK
                           and prep_patient_source.SiteCode = combined_data_ct_hts.SiteCode
               ),
                      
               combined_data_ct_hts_prep_mnch
               as
               (
                   select
                       coalesce(combined_data_ct_hts_prep.PatientPK, mnch_patient_source.PatientPK) as PatientPK,
                       coalesce(combined_data_ct_hts_prep.SiteCode, mnch_patient_source.SiteCode) as SiteCode,
                       combined_data_ct_hts_prep.CCCNumber,
                       coalesce(combined_data_ct_hts_prep.NUPI, mnch_patient_source.NUPI) as NUPI,
                       coalesce(combined_data_ct_hts_prep.DOB, mnch_patient_source.DOB) as DOB,
                       coalesce(combined_data_ct_hts_prep.Gender, mnch_patient_source.Gender) as Gender,
                       coalesce(combined_data_ct_hts_prep.PKV, mnch_patient_source.PKV) as PKV,
                       iif(combined_data_ct_hts_prep.Docket is not null and mnch_patient_source.Docket is not null,
           	CONCAT_WS('|', combined_data_ct_hts_prep.Docket, mnch_patient_source.Docket),
           	coalesce(combined_data_ct_hts_prep.Docket, mnch_patient_source.Docket)) as Docket
                   from combined_data_ct_hts_prep full join mnch_patient_source on combined_data_ct_hts_prep.PatientPK = mnch_patient_source.PatientPk
                           and combined_data_ct_hts_prep.SiteCode = mnch_patient_source.SiteCode
               )
             ,
               verified_list
               as
               (
                   select PKV, Gender, DOB, NUPI, SiteCode, PatientPK, CCCNumber, docket
                   from combined_data_ct_hts_prep_mnch
                   WHERE NUPI IS NOT NULL
               )
                      
           ,
               new_patient_list
               as
               (
                   select vl.*
                   from verified_list vl
                       left join ODS.dbo.MPI_MatchingOutput cl on cl.patient_pk = vl.PatientPK and cl.site_code = vl.SiteCode
                   where cl.patient_pk is null
               )
           select *
           from new_patient_list
           """;
   private final String SQL_INSERT_MATCHING_NOTIFICATION = """
           INSERT INTO mpi_matching_notification(interactionDwhId,goldenId,topCandidate)
                                            VALUES (?,?,?)
           """;
   private static final Logger LOGGER = LogManager.getLogger(DWH.class);
   private static final String MSSQL_URL = String.format("jdbc:sqlserver://%s;encrypt=false;databaseName=%s", AppConfig.MSSQL_HOST, AppConfig.MSSQL_DATABASE);
   private static final String MSSQL_USER = AppConfig.MSSQL_USER;
   private static final String MSSQL_PASSWORD = AppConfig.MSSQL_PASSWORD;
   private Connection mssqlConn;

   private static final String POSTGRES_URL = "jdbc:postgresql://postgresql:5432/notifications_db";
   private static final String POSTGRES_USER = AppConfig.POSTGRES_USER;
   private static final String POSTGRES_PASSWORD = AppConfig.POSTGRES_PASSWORD;
   private Connection postgresConn;

   DWH() {
   }

   private boolean openMssqlConnection() {
      try {
         if (mssqlConn == null || !mssqlConn.isValid(0)) {
            if (mssqlConn != null) {
               mssqlConn.close();
            }
            mssqlConn = DriverManager.getConnection(MSSQL_URL, MSSQL_USER, MSSQL_PASSWORD);
            mssqlConn.setAutoCommit(true);
            return mssqlConn.isValid(0);
         }
         return true;
      } catch (SQLException e) {
         LOGGER.error(e.getLocalizedMessage(), e);
      }
      return false;
   }

   private boolean openPostgresConnection() {
      try {
         if (postgresConn == null || !postgresConn.isValid(0)) {
            if (postgresConn != null) {
               postgresConn.close();
            }
            postgresConn = DriverManager.getConnection(POSTGRES_URL, POSTGRES_USER, POSTGRES_PASSWORD);
            postgresConn.setAutoCommit(true);
            return postgresConn.isValid(0);
         }
         return true;
      } catch (SQLException e) {
         LOGGER.error(e.getLocalizedMessage(), e);
      }
      return false;
   }

   void backPatchKeys(
         final String dwlId,
         final String goldenId,
         final String encounterId,
         final String phoneticGivenName,
         final String phoneticFamilyName) {
      if (openPostgresConnection()) {
//         try {
            try (PreparedStatement pStmt = postgresConn.prepareStatement(SQL_UPDATE, Statement.RETURN_GENERATED_KEYS)) {
               final PGobject uuid = new PGobject();
               uuid.setType("uuid");
               uuid.setValue(dwlId);
               pStmt.setString(1, goldenId);
               pStmt.setString(2, encounterId);
               pStmt.setString(3, phoneticGivenName.isEmpty() ? null : phoneticGivenName.toUpperCase());
               pStmt.setString(4, phoneticFamilyName.isEmpty() ? null : phoneticFamilyName.toUpperCase());
               pStmt.setObject(5, uuid);
               pStmt.executeUpdate();
//            }
         } catch (SQLException e) {
            LOGGER.error(e.getLocalizedMessage(), e);
         }
      } else {
         LOGGER.error("NO SQL SERVER");
      }
   }

   void insertMatchingNotifications(GoldenRecord goldenRecord, Interaction interaction, Boolean topCandidate) {
      if (openPostgresConnection()) {
         try (PreparedStatement pStmt = postgresConn.prepareStatement(SQL_INSERT_MATCHING_NOTIFICATION, Statement.RETURN_GENERATED_KEYS)) {
            String auxDwhId = interaction.uniqueInteractionData().auxDwhId();
            if (auxDwhId != null && !auxDwhId.isEmpty()) {
               pStmt.setInt(1, Integer.parseInt(auxDwhId));
               pStmt.setString(2, goldenRecord.goldenId());
               pStmt.setBoolean(3, topCandidate);
               pStmt.executeUpdate();
            }
         } catch (SQLException e) {
            LOGGER.error(e.getLocalizedMessage(), e);
         }
      } else {
         LOGGER.error("Unable to create DWH database connection");
      }

   }
   List<CustomPatientRecord> getPatientList(final String key, final SyncEvent event) {
      List<CustomPatientRecord> patientRecordList = new ArrayList<>();
      if (openMssqlConnection()) {
         try (Statement statement = mssqlConn.createStatement();
              ResultSet resultSet = statement.executeQuery(SQL_PATIENT_LIST)) {
            if (resultSet != null) {
               while (resultSet.next()) {
                  CustomPatientRecord patientRecord = new CustomPatientRecord(resultSet.getString("CCCNumber"),
                          resultSet.getString("PKV"), resultSet.getString("docket"),
                          resultSet.getString("Gender"), resultSet.getDate("DOB"),
                          resultSet.getString("NUPI"), resultSet.getString("SiteCode"), resultSet.getString("PatientPK"));
                  patientRecordList.add(patientRecord);
               }
            } else {
               LOGGER.info("Found empty result set for event {}, {}", event.event(), key);
            }
         } catch (SQLException e) {
            LOGGER.error(e.getLocalizedMessage(), e);
         }
      }
      return patientRecordList;
   }

   String insertClinicalData(
           final CustomDemographicData customDemographicData,
           final CustomSourceId customSourceId,
           final CustomUniqueInteractionData customUniqueInteractionData
           ) {
      String dwhId = null;
      if (openPostgresConnection()) {
//         try {
            try (PreparedStatement pStmt = postgresConn.prepareStatement(SQL_INSERT, Statement.RETURN_GENERATED_KEYS)) {
                  pStmt.setString(1, customDemographicData.getGender() == null || customDemographicData.getGender().isEmpty() ? null : customDemographicData.getGender());
                  pStmt.setString(2, customDemographicData.getDob() == null || customDemographicData.getDob().isEmpty() ? null : customDemographicData.getDob());
                  pStmt.setString(3, customDemographicData.getNupi() == null || customDemographicData.getNupi().isEmpty() ? null : customDemographicData.getNupi());
                  pStmt.setString(4, customDemographicData.getCccNumber() == null || customDemographicData.getCccNumber().isEmpty() ? null : customDemographicData.getCccNumber());
                  pStmt.setString(5, customSourceId.facility() == null || customSourceId.facility().isEmpty() ? null : customSourceId.facility());
                  pStmt.setString(6, customSourceId.patient() == null || customSourceId.patient().isEmpty() ? null : customSourceId.patient());
                  pStmt.setString(7, customUniqueInteractionData.pkv() == null || customUniqueInteractionData.pkv().isEmpty() ? null : customUniqueInteractionData.pkv());
                  pStmt.setString(8, customDemographicData.getDocket() == null || customDemographicData.getDocket().isEmpty() ? null : customDemographicData.getDocket());
               int affectedRows = pStmt.executeUpdate();
               if (affectedRows > 0) {
                  final var rs = pStmt.getGeneratedKeys();
                  if (rs.next()) {
                     dwhId = rs.getString(1);
                  }
               }
            } catch (SQLException e) {
            LOGGER.error(e.getLocalizedMessage(), e);
         }
//         } catch (SQLException e) {
//            LOGGER.error(e.getLocalizedMessage(), e);
//         }
      }
      return dwhId;
   }
}
