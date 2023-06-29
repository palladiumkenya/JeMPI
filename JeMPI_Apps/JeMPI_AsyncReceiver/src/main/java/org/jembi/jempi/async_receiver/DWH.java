//package org.jembi.jempi.async_receiver;
//
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//import org.postgresql.util.PGobject;
//
//import java.sql.*;
//
//final class DWH {
//
///*
//CREATE TABLE IF NOT EXISTS dwh (
//   dwh_id        UUID DEFAULT gen_random_uuid() PRIMARY KEY UNIQUE,
//   golden_id     VARCHAR(32),
//   encounter_id  VARCHAR(32),
//   pkv           VARCHAR(150),
//   site_code     VARCHAR(32),
//   patient_pk    VARCHAR(32),
//   nupi          VARCHAR(32)
//);
//*/
//
//   private static final String SQL_INSERT = """
//                                            INSERT INTO dwh(pkv,gender,dob,nupi,site_code,patient_pk,ccc_number)
//                                            VALUES (?,?,?,?,?,?,?)
//                                            """;
//
//
//   private static final String SQL_UPDATE = """
//                                            UPDATE dwh
//                                            SET golden_id = ?, encounter_id = ?
//                                            WHERE dwh_id = ?
//                                            """;
//   private static final Logger LOGGER = LogManager.getLogger(DWH.class);
//   private static final String URL = "jdbc:postgresql://postgresql:5432/notifications";
//   private static final String USER = "postgres";
//   private Connection conn;
//
//   DWH() {
//   }
//
//   private boolean open() {
//      try {
//         if (conn == null || !conn.isValid(0)) {
//            if (conn != null) {
//               conn.close();
//            }
//            conn = DriverManager.getConnection(URL, USER, null);
//            conn.setAutoCommit(true);
//            return conn.isValid(0);
//         }
//         return true;
//      } catch (SQLException e) {
//         LOGGER.error(e.getLocalizedMessage(), e);
//      }
//      return false;
//   }
//
//   void backPatchKeys(
//         final String dwlId,
//         final String goldenId,
//         final String encounterId) {
//      if (open()) {
//         try {
//            try (PreparedStatement pStmt = conn.prepareStatement(SQL_UPDATE, Statement.RETURN_GENERATED_KEYS)) {
//               final PGobject uuid = new PGobject();
//               uuid.setType("uuid");
//               uuid.setValue(dwlId);
//               pStmt.setString(1, goldenId);
//               pStmt.setString(2, encounterId);
//               pStmt.setObject(3, uuid);
//               pStmt.executeUpdate();
//            }
//         } catch (SQLException e) {
//            LOGGER.error(e.getLocalizedMessage(), e);
//         }
//      } else {
//         LOGGER.error("NO SQL SERVER");
//      }
//   }
//
//   String insertClinicalData(
//         final String pkv,
//         final String gender,
//         final String dob,
//         final String nupi,
//         final String siteCode,
//         final String patientPk,
//         final String cccNumber
//         ) {
//      LOGGER.debug("{} {} {} {} {} {} {}", pkv, gender, dob, nupi, siteCode, patientPk, cccNumber);
//      String dwhId = null;
//      if (open()) {
//         try {
//            if (conn == null || !conn.isValid(0)) {
//               if (conn != null) {
//                  conn.close();
//               }
//               open();
//            }
//            try (PreparedStatement pStmt = conn.prepareStatement(SQL_INSERT, Statement.RETURN_GENERATED_KEYS)) {
//                  pStmt.setString(1, pkv == null || pkv.isEmpty() ? null : pkv);
//                  pStmt.setString(2, gender == null || gender.isEmpty() ? null : gender);
//                  pStmt.setString(3, dob == null || dob.isEmpty() ? null : dob);
//                  pStmt.setString(4, nupi == null || nupi.isEmpty() ? null : nupi);
//                  pStmt.setString(5, siteCode == null || siteCode.isEmpty() ? null : siteCode);
//                  pStmt.setString(6, patientPk == null || patientPk.isEmpty() ? null : patientPk);
//                  pStmt.setString(7, cccNumber == null || cccNumber.isEmpty() ? null : cccNumber);
//               int affectedRows = pStmt.executeUpdate();
//               if (affectedRows > 0) {
//                  final var rs = pStmt.getGeneratedKeys();
//                  if (rs.next()) {
//                     dwhId = rs.getString(1);
//                  }
//               }
//            }
//         } catch (SQLException e) {
//            LOGGER.error(e.getLocalizedMessage(), e);
//         }
//      }
//      return dwhId;
//   }
//
//}
