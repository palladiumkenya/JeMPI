package org.jembi.jempi.async_receiver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.postgresql.util.PGobject;

import java.sql.*;

final class DWH {
   private static final String SQL_INSERT = """
                                            INSERT INTO dwh(clinical_data)
                                            VALUES (?)
                                            """;

   private static final String SQL_UPDATE = """
                                            UPDATE dwh
                                            SET golden_id = ?, encounter_id = ?
                                            WHERE dwh_id = ?
                                            """;
   private static final Logger LOGGER = LogManager.getLogger(DWH.class);
   private static final String URL = "jdbc:postgresql://postgresql:5432/notifications";
   private static final String USER = "postgres";
   private Connection conn;

   DWH() {
      try {
         conn = DriverManager.getConnection(URL, USER, null);
         conn.setAutoCommit(true);
      } catch (SQLException e) {
         LOGGER.error(e.getLocalizedMessage(), e);
      }
   }

   void updateKeys(
         final String dwlId,
         final String goldenId,
         final String encounterId) {
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
   }

   String insertClinicalData(final String clinicalData) {
      String dwhId = null;
      try {
         try (PreparedStatement pStmt = conn.prepareStatement(SQL_INSERT, Statement.RETURN_GENERATED_KEYS)) {
            pStmt.setString(1, clinicalData);
            int affectedRows = pStmt.executeUpdate();
            if (affectedRows > 0) {
               final var rs = pStmt.getGeneratedKeys();
               if (rs.next()) {
                  dwhId = rs.getString(1);
               }
            }
         }
      } catch (SQLException e) {
         LOGGER.error(e.getLocalizedMessage(), e);
      }
      return dwhId;
   }

}
