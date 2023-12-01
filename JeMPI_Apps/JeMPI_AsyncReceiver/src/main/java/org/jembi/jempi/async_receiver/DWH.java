package org.jembi.jempi.async_receiver;

import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jembi.jempi.AppConfig;
import org.jembi.jempi.shared.kafka.MyKafkaProducer;
import org.jembi.jempi.shared.models.*;
import org.jembi.jempi.shared.serdes.JsonPojoSerializer;

import java.sql.*;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

final class DWH {
   private MyKafkaProducer<String, InteractionEnvelop> interactionEnvelopProducer;
   private static final String SQL_INSERT = """
                                            INSERT INTO dwh(gender,dob,nupi,ccc_number,site_code,patient_pk,pkv,docket)
                                            VALUES (?,?,?,?,?,?,?,?)
                                            """;


   private static final String SQL_UPDATE = """
                                            UPDATE dwh
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
                       left join notifications.dbo.dwh cl on cl.patient_pk = vl.PatientPK and cl.site_code = vl.SiteCode
                   where cl.patient_pk is null
               )
           select *
           from new_patient_list
           """;
   private static final Logger LOGGER = LogManager.getLogger(DWH.class);
   private static final String URL = String.format("jdbc:sqlserver://%s;encrypt=false;databaseName=%s", AppConfig.MSSQL_HOST, AppConfig.MSSQL_DATABASE);
   private static final String USER = AppConfig.MSSQL_USER;

   private static final String PASSWORD = AppConfig.MSSQL_PASSWORD;
   private Connection conn;

   DWH() {
   }

   void initiateProducer() {
      interactionEnvelopProducer = new MyKafkaProducer<>(AppConfig.KAFKA_BOOTSTRAP_SERVERS,
              GlobalConstants.TOPIC_INTERACTION_ASYNC_ETL,
              new StringSerializer(), new JsonPojoSerializer<>(),
              "dwh-async-client-id-syncrx");
   }

   private boolean open() {
      try {
         if (conn == null || !conn.isValid(0)) {
            if (conn != null) {
               conn.close();
            }
            conn = DriverManager.getConnection(URL, USER, PASSWORD);
            conn.setAutoCommit(true);
            return conn.isValid(0);
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
      if (open()) {
         try {
            try (PreparedStatement pStmt = conn.prepareStatement(SQL_UPDATE, Statement.RETURN_GENERATED_KEYS)) {
               pStmt.setString(1, goldenId);
               pStmt.setString(2, encounterId);
               pStmt.setString(3, phoneticGivenName.isEmpty() ? null : phoneticGivenName.toUpperCase());
               pStmt.setString(4, phoneticFamilyName.isEmpty() ? null : phoneticFamilyName.toUpperCase());
               pStmt.setInt(5, Integer.parseInt(dwlId));
               pStmt.executeUpdate();
            }
         } catch (SQLException e) {
            LOGGER.error(e.getLocalizedMessage(), e);
         }
      } else {
         LOGGER.error("NO SQL SERVER");
      }
   }


   void syncPatientList(final String key, final SyncEvent event) throws InterruptedException, ExecutionException {
      if (open()) {
         try (Statement statement = conn.createStatement()) {
            statement.setQueryTimeout(3600);
            ResultSet resultSet = statement.executeQuery(SQL_PATIENT_LIST);
            if (resultSet != null) {
               final var dtf = DateTimeFormatter.ofPattern("uuuu/MM/dd HH:mm:ss");
               final var now = LocalDateTime.now();
               final var stanDate = dtf.format(now);
               final var uuid = UUID.randomUUID().toString();
               int index = 0;
               sendToKafka(uuid, new InteractionEnvelop(InteractionEnvelop.ContentType.BATCH_START_SENTINEL, stanDate,
                       String.format(Locale.ROOT, "%s:%07d", stanDate, ++index), null));
               while (resultSet.next()) {
                  CustomUniqueInteractionData uniqueInteractionData = new CustomUniqueInteractionData(java.time.LocalDateTime.now(),
                          null, resultSet.getString("CCCNumber"), resultSet.getString("docket"),
                          resultSet.getString("PKV"), null);
                  CustomDemographicData demographicData = new CustomDemographicData(null, null,
                          resultSet.getString("Gender"), resultSet.getDate("DOB").toString(),
                          resultSet.getString("NUPI"));
                  CustomSourceId sourceId = new CustomSourceId(null, resultSet.getString("SiteCode"), resultSet.getString("PatientPK"));
                  LOGGER.info("Persisting record {} {}", sourceId.patient(), sourceId.facility());
                  String dwhId = insertClinicalData(demographicData, sourceId, uniqueInteractionData);

                  if (dwhId == null) {
                     LOGGER.warn("Failed to insert record sc({}) pk({})", sourceId.facility(), sourceId.patient());
                  }
                  uniqueInteractionData = new CustomUniqueInteractionData(uniqueInteractionData.auxDateCreated(),
                          null, uniqueInteractionData.cccNumber(), uniqueInteractionData.docket(), uniqueInteractionData.pkv(), dwhId);
                  LOGGER.debug("Inserted record with dwhId {}", uniqueInteractionData.auxDwhId());
                  sendToKafka(UUID.randomUUID().toString(),
                          new InteractionEnvelop(InteractionEnvelop.ContentType.BATCH_INTERACTION, stanDate,
                                  String.format(Locale.ROOT, "%s:%07d", stanDate, ++index),
                                  new Interaction(null,
                                          sourceId,
                                          uniqueInteractionData,
                                          demographicData)));
               }
               sendToKafka(uuid, new InteractionEnvelop(InteractionEnvelop.ContentType.BATCH_END_SENTINEL, stanDate,
                       String.format(Locale.ROOT, "%s:%07d", stanDate, ++index), null));
               LOGGER.info("Synced {} patient records", index);
            } else {
               LOGGER.info("Found empty result set for event {}, {}", event.event(), key);
            }
         } catch (SQLException e) {
            LOGGER.error(e.getLocalizedMessage(), e);
         }
      }
   }

   private void sendToKafka(
           final String key,
           final InteractionEnvelop interactionEnvelop)
           throws InterruptedException, ExecutionException {
      try {
         interactionEnvelopProducer.produceSync(key, interactionEnvelop);
      } catch (NullPointerException ex) {
         LOGGER.error(ex.getLocalizedMessage(), ex);
      }
   }


   String insertClinicalData(
           final CustomDemographicData customDemographicData,
           final CustomSourceId customSourceId,
           final CustomUniqueInteractionData customUniqueInteractionData
           ) {
      String dwhId = null;
      if (open()) {
         try {
            if (conn == null || !conn.isValid(0)) {
               if (conn != null) {
                  conn.close();
               }
               open();
            }
            try (PreparedStatement pStmt = conn.prepareStatement(SQL_INSERT, Statement.RETURN_GENERATED_KEYS)) {
                  pStmt.setString(1, customDemographicData.getGender().isEmpty() ? null : customDemographicData.getGender());
                  pStmt.setString(2, customDemographicData.getDob().isEmpty() ? null : customDemographicData.getDob());
                  pStmt.setString(3, customDemographicData.getNupi().isEmpty() ? null : customDemographicData.getNupi());
                  pStmt.setString(4, customUniqueInteractionData.cccNumber().isEmpty() ? null : customUniqueInteractionData.cccNumber());
                  pStmt.setString(5, customSourceId.facility().isEmpty() ? null : customSourceId.facility());
                  pStmt.setString(6, customSourceId.patient().isEmpty() ? null : customSourceId.patient());
                  pStmt.setString(7, customUniqueInteractionData.pkv().isEmpty() ? null : customUniqueInteractionData.pkv());
                  pStmt.setString(8, customUniqueInteractionData.docket().isEmpty() ? null : customUniqueInteractionData.docket());
               int affectedRows = pStmt.executeUpdate();
               if (affectedRows > 0) {
                  final var rs = pStmt.getGeneratedKeys();
                  if (rs.next()) {
                     dwhId = Integer.toString(rs.getInt(1));
                  }
               }
            }
         } catch (SQLException e) {
            LOGGER.error(e.getLocalizedMessage(), e);
         }
      }
      return dwhId;
   }

}
