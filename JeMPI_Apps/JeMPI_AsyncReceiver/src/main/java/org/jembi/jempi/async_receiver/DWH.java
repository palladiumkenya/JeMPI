package org.jembi.jempi.async_receiver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jembi.jempi.AppConfig;
import org.jembi.jempi.shared.models.CustomDemographicData;
import org.jembi.jempi.shared.models.CustomSourceId;
import org.postgresql.util.PGobject;

import java.sql.*;

final class DWH {

/*
CREATE TABLE notifications.dbo.dwh
(
     dwh_id        INT IDENTITY(1,1) PRIMARY KEY,
     golden_id     VARCHAR(32),
     encounter_id  VARCHAR(32),
     phonetic_given_name    VARCHAR(10),
     phonetic_family_name   VARCHAR(10),
     gender        VARCHAR(32),
     dob           VARCHAR(32),
     nupi          VARCHAR(32),
     ccc_number    VARCHAR(150),
     site_code     VARCHAR(32),
     patient_pk    VARCHAR(32)
);
*/

   private static final String SQL_INSERT = """
                                            INSERT INTO dwh(phonetic_given_name,phonetic_family_name,gender,dob,nupi,ccc_number,site_code,patient_pk)
                                            VALUES (?,?,?,?,?,?,?,?)
                                            """;


   private static final String SQL_UPDATE = """
                                            UPDATE dwh
                                            SET golden_id = ?, encounter_id = ?
                                            WHERE dwh_id = ?
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
         final String encounterId) {
      if (open()) {
         try {
            try (PreparedStatement pStmt = conn.prepareStatement(SQL_UPDATE, Statement.RETURN_GENERATED_KEYS)) {
               final PGobject uuid = new PGobject();
               uuid.setType("uuid");
               uuid.setValue(dwlId);
               pStmt.setString(1, goldenId);
               pStmt.setString(2, encounterId);
               pStmt.setObject(3, uuid);
               pStmt.executeUpdate();
            }
         } catch (SQLException e) {
            LOGGER.error(e.getLocalizedMessage(), e);
         }
      } else {
         LOGGER.error("NO SQL SERVER");
      }
   }

   String insertClinicalData(
           final CustomDemographicData customDemographicData,
           final CustomSourceId customSourceId
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
                  pStmt.setString(1, customDemographicData.getPhoneticGivenName().isEmpty() ? null : customDemographicData.getPhoneticGivenName());
                  pStmt.setString(2, customDemographicData.getPhoneticFamilyName().isEmpty() ? null : customDemographicData.getPhoneticFamilyName());
                  pStmt.setString(3, customDemographicData.getGender().isEmpty() ? null : customDemographicData.getGender());
                  pStmt.setString(4, customDemographicData.getDob().isEmpty() ? null : customDemographicData.getDob());
                  pStmt.setString(5, customDemographicData.getNupi().isEmpty() ? null : customDemographicData.getNupi());
                  pStmt.setString(6, customDemographicData.getCccNumber().isEmpty() ? null : customDemographicData.getCccNumber());
                  pStmt.setString(7, customSourceId.facility().isEmpty() ? null : customSourceId.facility());
                  pStmt.setString(8, customSourceId.patient().isEmpty() ? null : customSourceId.patient());
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
