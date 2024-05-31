package org.jembi.jempi.async_receiver;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jembi.jempi.AppConfig;
import org.jembi.jempi.shared.models.*;
import org.postgresql.util.PGobject;

import java.sql.*;

public final class NotificationDao {
    private static final String SQL_INSERT = """
                                            INSERT INTO mpi_matching_output(gender,dob,nupi,ccc_number,site_code,patient_pk,pkv,docket)
                                            VALUES (?,?,?,?,?,?,?,?)
                                            """;
    private static final String SQL_UPDATE = """
                                            UPDATE mpi_matching_output
                                            SET golden_id = ?, encounter_id = ?, phonetic_given_name = ?, phonetic_family_name = ?
                                            WHERE dwh_id = ?
                                            """;
    private static final String SQL_INSERT_MATCHING_NOTIFICATION = """
                                                            INSERT INTO mpi_matching_notification(interaction_dwh_id,golden_id,top_candidate)
                                                            VALUES (?,?,?)
                                                            """;
    private static final String SQL_INSERT_MATCHING_VALIDATION = """
                                                         INSERT INTO mpi_failed_validation(interaction_dwh_id)
                                                         VALUES(?)
                                                         """;
    private static final Logger LOGGER = LogManager.getLogger(NotificationDao.class);
    private static final String URL = "jdbc:postgresql://postgresql:5432/notifications_db";
    private static final String USER = AppConfig.POSTGRES_USER;
    private static final String PASSWORD = AppConfig.POSTGRES_PASSWORD;
    private Connection connection;

    public NotificationDao() {
    }

    private boolean openConnection() {
        try {
            if (connection == null || !connection.isValid(0)) {
                if (connection != null) {
                    connection.close();
                }
                connection = DriverManager.getConnection(URL, USER, PASSWORD);
                connection.setAutoCommit(true);
                return connection.isValid(0);
            }
            return true;
        } catch (SQLException e) {
            LOGGER.error(e.getLocalizedMessage(), e);
        }
        return false;
    }

    String insertClinicalData(
            final CustomDemographicData customDemographicData,
            final CustomSourceId customSourceId,
            final CustomUniqueInteractionData customUniqueInteractionData
    ) {
        String dwhId = null;
        if (openConnection()) {
            try {
                try (PreparedStatement pStmt = connection.prepareStatement(SQL_INSERT, Statement.RETURN_GENERATED_KEYS)) {
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
                        try (var rs = pStmt.getGeneratedKeys()) {
                            if (rs.next()) {
                                dwhId = rs.getString(1);
                            }
                        }
                    }
                } catch (SQLException e) {
                    LOGGER.error(e.getLocalizedMessage(), e);
                }
            } catch (Exception e) {
                LOGGER.error(e.getLocalizedMessage(), e);
            }
        }
        return dwhId;
    }

    void backPatchKeys(
            final String dwlId,
            final String goldenId,
            final String encounterId,
            final String phoneticGivenName,
            final String phoneticFamilyName) {
        if (openConnection()) {
            try {
                try (PreparedStatement pStmt = connection.prepareStatement(SQL_UPDATE, Statement.RETURN_GENERATED_KEYS)) {
                    final PGobject uuid = new PGobject();
                    uuid.setType("uuid");
                    uuid.setValue(dwlId);
                    pStmt.setString(1, goldenId);
                    pStmt.setString(2, encounterId);
                    pStmt.setString(3, phoneticGivenName.isEmpty() ? null : phoneticGivenName.toUpperCase());
                    pStmt.setString(4, phoneticFamilyName.isEmpty() ? null : phoneticFamilyName.toUpperCase());
                    pStmt.setObject(5, uuid);
                    pStmt.executeUpdate();
                } catch (SQLException se) {
                    LOGGER.error(se.getLocalizedMessage(), se);
                }
            } catch (Exception e) {
                LOGGER.error(e.getLocalizedMessage(), e);
            }
        }
    }

    void insertMatchingNotifications(final GoldenRecord goldenRecord, final Interaction interaction, final Boolean topCandidate) {
        if (openConnection()) {
            try (PreparedStatement pStmt = connection.prepareStatement(SQL_INSERT_MATCHING_NOTIFICATION, Statement.RETURN_GENERATED_KEYS)) {
                String auxDwhId = interaction.uniqueInteractionData().auxDwhId();
                final PGobject uuid = new PGobject();
                uuid.setType("uuid");
                uuid.setValue(auxDwhId);

                if (auxDwhId != null && !auxDwhId.isEmpty()) {
                    pStmt.setObject(1, uuid);
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

    void insertValidationNotification(final String dwhId) {
        if (openConnection()) {
            try (PreparedStatement pStmt = connection.prepareStatement(SQL_INSERT_MATCHING_VALIDATION)) {
                final PGobject uuid = new PGobject();
                uuid.setType("uuid");
                uuid.setValue(dwhId);
                pStmt.setObject(1, uuid);
                pStmt.executeUpdate();
            } catch (SQLException e) {
                LOGGER.error(e.getLocalizedMessage(), e);
            }
        }
    }
}
