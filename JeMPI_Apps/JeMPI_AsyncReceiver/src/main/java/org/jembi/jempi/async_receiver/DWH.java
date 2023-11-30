package org.jembi.jempi.async_receiver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jembi.jempi.AppConfig;
import org.jembi.jempi.shared.models.CustomDemographicData;
import org.jembi.jempi.shared.models.CustomSourceId;
import org.jembi.jempi.shared.models.CustomUniqueInteractionData;

import java.sql.*;

final class DWH {
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
                      			else  NUPIs
                      			end as NUPI,
                       Pkv,
                       'C&T' as Docket
              \s
              \s
                              from
                              ODS.dbo.CT_Patient as patients
                      	)
                                                                          \s
                          ,hts_patient_source as
           (
                              select s
           distinct
                                  PatientPK,
                                  SiteCode,
                                  case
                      			when Gender = 'F' then 'Female'
                      			when Gender = 'M' then 'Male'
                      			when Gender = '' then NULL
                      			else Gender
           end as Gender,
                                  cast
           (DOB as date) as DOB,
                      			case
                      			when NUPI = '' then NULL
                      			else  NUPIs
           end as NUPI,
                      			PKV,
                      			'HTS' as Docket
                              from ODS.dbo.HTS_clients as clients
                          )
                                                                          \s
                          ,prep_patient_source as
           (
                          selects
                              distinct
                              PatientPk,
                              PrepNumber,
                              SiteCode,
                              Sex AS Gender,
                              cast
           (DateofBirth as date) as DOB,
                      		'PrEP' as Docket
                          from ODS.dbo.PrEP_Patient
                          )
                                                                          \s
                         ,mnch_patient_source as
           (
                          selects
                              distinct
                              PatientPk,
                              SiteCode,
                              Gender,
                              cast
           (DOB as date) as DOB,
                      		case
                      		when NUPI = '' then NULL
                      		else  NUPIs
           end as NUPI,
                      		Pkv,
                      		'MNCH' as Docket
                          from ODS.dbo.MNCH_Patient
                          )
                                                                          \s
                      	,combined_data_ct_hts as
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
                      	)
           ,
                                                                          \s
                      	combined_data_ct_hts_prep as
           (
                      	selects
                      	coalesce
           (combined_data_ct_hts.PatientPK, prep_patient_source.PatientPK) as PatientPK,
                      	coalesce
           (combined_data_ct_hts.SiteCode, prep_patient_source.SiteCode) as SiteCode,
                      	combined_data_ct_hts.CCCNumber,
                          combined_data_ct_hts.NUPI as NUPI,
                          coalesce
           (combined_data_ct_hts.DOB, prep_patient_source.DOB) as DOB,
                      	coalesce
           (combined_data_ct_hts.Gender, prep_patient_source.Gender) as Gender,
                      	combined_data_ct_hts.PKV,
                      	iif
           (combined_data_ct_hts.Docket is not null and prep_patient_source.Docket is not null,
                      	CONCAT_WS
           ('|', combined_data_ct_hts.Docket, prep_patient_source.Docket),
                      	coalesce
           (combined_data_ct_hts.Docket, prep_patient_source.Docket)) as Docket
                      	from combined_data_ct_hts
                      	full join prep_patient_source on combined_data_ct_hts.PatientPK = prep_patient_source.PatientPK
                                  and prep_patient_source.SiteCode = combined_data_ct_hts.SiteCode
                      	),
                      	
                      	combined_data_ct_hts_prep_mnch as
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
           , verified_list as
           (
                          select PKV, Gender, DOB, NUPI, SiteCode, PatientPK, CCCNumber, docket
           from combined_data_ct_hts_prep_mnch
           WHERE NUPI IS NOT NULL
                        )
                                                                
           ,new_patient_list as
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


   ResultSet getPatientList() {
      ResultSet resultSet = null;
      if (open()) {
         try (Statement statement = conn.createStatement()) {
            resultSet = statement.executeQuery(SQL_PATIENT_LIST);
         } catch (SQLException e) {
            LOGGER.error(e.getLocalizedMessage(), e);
         }
      }
      return resultSet;
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
