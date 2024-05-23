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
   private static final String SQL_PATIENT_QUERY_NAME = "patient_list.sql";
   private static final String SQL_PATIENT_LIST = "";
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
      Path queryPath = customPath == null ? getDefaultQueryPath() : customPath;
      try (BufferedReader br = new BufferedReader(new InputStreamReader(Files.newInputStream(queryPath), StandardCharsets.UTF_8))) {
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
      Path queryPath = Paths.get("/app/sql/"+ SQL_PATIENT_QUERY_NAME);
      if (Files.exists(queryPath)) {
         return queryPath;
      }
      return null;
   }
   private Path getDefaultQueryPath() {
      Path path = null;
      URL url = (NdwDao.class.getClassLoader().getResource(SQL_PATIENT_QUERY_NAME));
      if (url != null) {
         try {
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
