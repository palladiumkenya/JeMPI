package org.jembi.jempi.async_receiver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jembi.jempi.AppConfig;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;

final class NdwDao {
   private static final String SQL_PATIENT_QUERY_FILE_NAME = "patient_list.sql";
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
                     \s
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
                     \s
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
                     \s
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
                     \s
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
                     \s
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
                     \s
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
                     \s
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
   private static final Logger LOGGER = LogManager.getLogger(NdwDao.class);
   private static final String URL = String.format("jdbc:sqlserver://%s;encrypt=false;databaseName=%s", AppConfig.MSSQL_HOST, AppConfig.MSSQL_DATABASE);
   private static final String USER = AppConfig.MSSQL_USER;
   private static final String PASSWORD = AppConfig.MSSQL_PASSWORD;
   private Connection conn;

   NdwDao() {
   }

   private boolean openConnection() {
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

   private String getFetchPatientsQuery() throws IOException {
      Path customPath = getCustomQueryPath();
      if (customPath == null) {
         return SQL_PATIENT_LIST;
      }
      try (BufferedReader br = new BufferedReader(new InputStreamReader(Files.newInputStream(customPath), StandardCharsets.UTF_8))) {
         StringBuilder queryBuilder = new StringBuilder();
         String line;
         while ((line = br.readLine()) != null) {
            queryBuilder.append(line);
            queryBuilder.append("\n");
         }
         String query = queryBuilder.toString();
         LOGGER.debug("Fetch patient list query: {}", query);
         return query;
      }
   }
   private Path getCustomQueryPath() {
      Path queryPath = Paths.get("/app/sql/"+ SQL_PATIENT_QUERY_FILE_NAME);
      if (Files.exists(queryPath)) {
         return queryPath;
      }
      return null;
   }
   private Path getDefaultQueryPath() {
      Path path = null;
      URL url = NdwDao.class.getClassLoader().getResource(SQL_PATIENT_QUERY_FILE_NAME);
      if (url != null) {
         try {
            LOGGER.info("Resource path for query: {}", url.getPath());
            path = Paths.get(url.toURI());
         } catch (URISyntaxException e) {
            LOGGER.error(e.getLocalizedMessage(), e);
         }
      }
      return path;
   }

   List<CustomPatientRecord> getPatientList() {
      List<CustomPatientRecord> patientRecordList = new ArrayList<>();
      if (openConnection()) {
            try (Statement statement = conn.createStatement();
                 ResultSet resultSet = statement.executeQuery(getFetchPatientsQuery())) {
                  while (resultSet.next()) {
                     CustomPatientRecord patientRecord = new CustomPatientRecord(resultSet.getString("CCCNumber"),
                             resultSet.getString("PKV"), resultSet.getString("docket"),
                             resultSet.getString("Gender"), resultSet.getDate("DOB"),
                             resultSet.getString("NUPI"), resultSet.getString("SiteCode"), resultSet.getString("PatientPK"));
                     patientRecordList.add(patientRecord);
                  }
            } catch (SQLException | IOException e) {
               LOGGER.error(e.getLocalizedMessage(), e);
            }
      }
      return patientRecordList;
   }
}
