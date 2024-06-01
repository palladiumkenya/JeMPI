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
           WITH ct_patient_source
           AS (SELECT DISTINCT
                   patients.patientid AS cccnumber,
                   patients.patientpk,
                   patients.sitecode,
                   CASE
                       WHEN gender = 'F' THEN
                           'Female'
                       WHEN gender = 'M' THEN
                           'Male'
                       WHEN gender = '' THEN
                           NULL
                       ELSE
                           gender
                   END AS gender,
                   Cast(dob AS DATE) AS dob,
                   CASE
                       WHEN nupi = '' THEN
                           NULL
                       ELSE
                           nupi
                   END AS nupi,
                   pkv,
                   'C&T' AS docket
               FROM ods.dbo.ct_patient AS patients
              ),
                hts_patient_source
           AS (SELECT DISTINCT
                   patientpk,
                   sitecode,
                   CASE
                       WHEN gender = 'F' THEN
                           'Female'
                       WHEN gender = 'M' THEN
                           'Male'
                       WHEN gender = '' THEN
                           NULL
                       ELSE
                           gender
                   END AS gender,
                   cast(dob AS date) AS dob,
                   CASE
                       WHEN nupi = '' THEN
                           NULL
                       ELSE
                           nupi
                   END AS nupi,
                   pkv,
                   'HTS' AS docket
               FROM ods.dbo.hts_clients AS clients
              ),
                prep_patient_source
           AS (SELECT DISTINCT
                   patientpk,
                   prepnumber,
                   sitecode,
                   sex AS gender,
                   cast(dateofbirth AS date) AS dob,
                   'PrEP' AS docket
               FROM ods.dbo.prep_patient
              ),
                mnch_patient_source
           AS (SELECT DISTINCT
                   patientpk,
                   sitecode,
                   gender,
                   cast(dob AS date) AS dob,
                   CASE
                       WHEN nupi = '' THEN
                           NULL
                       ELSE
                           nupi
                   END AS nupi,
                   pkv,
                   'MNCH' AS docket
               FROM ods.dbo.mnch_patient
              ),
                combined_data_ct_hts
           AS (SELECT COALESCE(ct_patient_source.patientpk, hts_patient_source.patientpk) AS patientpk,
                      COALESCE(ct_patient_source.sitecode, hts_patient_source.sitecode) AS sitecode,
                      ct_patient_source.cccnumber,
                      COALESCE(ct_patient_source.nupi, hts_patient_source.nupi) AS nupi,
                      COALESCE(ct_patient_source.dob, hts_patient_source.dob) AS dob,
                      COALESCE(ct_patient_source.gender, hts_patient_source.gender) AS gender,
                      COALESCE(ct_patient_source.pkv, hts_patient_source.pkv) AS pkv,
                      iif(
                          ct_patient_source.docket IS NOT NULL
                          AND hts_patient_source.docket IS NOT NULL,
                          concat_ws('|', ct_patient_source.docket, hts_patient_source.docket),
                          COALESCE(ct_patient_source.docket, hts_patient_source.docket)) AS docket
               FROM ct_patient_source
                   FULL JOIN hts_patient_source
                       ON hts_patient_source.patientpk = ct_patient_source.patientpk
                          AND ct_patient_source.sitecode = hts_patient_source.sitecode
              ),
                combined_data_ct_hts_prep
           AS (SELECT COALESCE(combined_data_ct_hts.patientpk, prep_patient_source.patientpk) AS patientpk,
                      COALESCE(combined_data_ct_hts.sitecode, prep_patient_source.sitecode) AS sitecode,
                      combined_data_ct_hts.cccnumber,
                      combined_data_ct_hts.nupi AS nupi,
                      COALESCE(combined_data_ct_hts.dob, prep_patient_source.dob) AS dob,
                      COALESCE(combined_data_ct_hts.gender, prep_patient_source.gender) AS gender,
                      combined_data_ct_hts.pkv,
                      iif(
                          combined_data_ct_hts.docket IS NOT NULL
                          AND prep_patient_source.docket IS NOT NULL,
                          concat_ws('|', combined_data_ct_hts.docket, prep_patient_source.docket),
                          COALESCE(combined_data_ct_hts.docket, prep_patient_source.docket)) AS docket
               FROM combined_data_ct_hts
                   FULL JOIN prep_patient_source
                       ON combined_data_ct_hts.patientpk = prep_patient_source.patientpk
                          AND prep_patient_source.sitecode = combined_data_ct_hts.sitecode
              ),
                combined_data_ct_hts_prep_mnch
           AS (SELECT COALESCE(combined_data_ct_hts_prep.patientpk, mnch_patient_source.patientpk) AS patientpk,
                      COALESCE(combined_data_ct_hts_prep.sitecode, mnch_patient_source.sitecode) AS sitecode,
                      combined_data_ct_hts_prep.cccnumber,
                      COALESCE(combined_data_ct_hts_prep.nupi, mnch_patient_source.nupi) AS nupi,
                      COALESCE(combined_data_ct_hts_prep.dob, mnch_patient_source.dob) AS dob,
                      COALESCE(combined_data_ct_hts_prep.gender, mnch_patient_source.gender) AS gender,
                      COALESCE(combined_data_ct_hts_prep.pkv, mnch_patient_source.pkv) AS pkv,
                      iif(
                          combined_data_ct_hts_prep.docket IS NOT NULL
                          AND mnch_patient_source.docket IS NOT NULL,
                          concat_ws('|', combined_data_ct_hts_prep.docket, mnch_patient_source.docket),
                          COALESCE(combined_data_ct_hts_prep.docket, mnch_patient_source.docket)) AS docket
               FROM combined_data_ct_hts_prep
                   FULL JOIN mnch_patient_source
                       ON combined_data_ct_hts_prep.patientpk = mnch_patient_source.patientpk
                          AND combined_data_ct_hts_prep.sitecode = mnch_patient_source.sitecode
              ),
                verified_list
           AS (SELECT pkv,
                      gender,
                      dob,
                      nupi,
                      sitecode,
                      patientpk,
                      cccnumber,
                      docket
               FROM combined_data_ct_hts_prep_mnch
               WHERE nupi IS NOT NULL
              ),
                new_patient_list
           AS (SELECT vl.*
               FROM verified_list vl
                   LEFT JOIN ods.dbo.mpi_matchingoutput cl
                       ON cl.patient_pk = vl.patientpk
                          AND cl.site_code = vl.sitecode
               WHERE cl.patient_pk IS NULL
              )
           SELECT *
           FROM new_patient_list
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
      Path queryPath = Paths.get("/app/sql/" + SQL_PATIENT_QUERY_FILE_NAME);
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
