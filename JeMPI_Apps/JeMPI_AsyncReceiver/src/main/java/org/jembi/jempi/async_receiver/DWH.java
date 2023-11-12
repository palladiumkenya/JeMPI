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
