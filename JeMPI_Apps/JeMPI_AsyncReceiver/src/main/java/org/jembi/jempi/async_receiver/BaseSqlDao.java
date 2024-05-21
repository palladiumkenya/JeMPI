//package org.jembi.jempi.async_receiver;
//
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.SQLException;
//
//public abstract class BaseSqlDao {
//
//    protected String url;
//    protected String user;
//    protected String password;
//    protected Connection connection;
//    private static final Logger LOGGER = LogManager.getLogger(NdwDao.class);
//
//    abstract void initializeProperties();
//
//    private boolean openConnection() {
//        try {
//            if (connection == null || !connection.isValid(0)) {
//                if (connection != null) {
//                    connection.close();
//                }
//                connection = DriverManager.getConnection(url, user, password);
//                connection.setAutoCommit(true);
//                return connection.isValid(0);
//            }
//            return true;
//        } catch (SQLException e) {
//            LOGGER.error(e.getLocalizedMessage(), e);
//        }
//        return false;
//    }
//}
