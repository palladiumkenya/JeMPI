//package org.jembi.jempi.async_receiver;
//
//import org.apache.logging.log4j.LogManager;
//import org.apache.logging.log4j.Logger;
//
//import java.sql.Connection;
//import java.sql.DriverManager;
//import java.sql.SQLException;
//
//public class BaseSqlDao {
//    private String url;
//    private String user;
//    private String password;
//    private Connection connection;
//    static final Logger LOGGER = LogManager.getLogger(BaseSqlDao.class);
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
//
//    public String getUrl() {
//        return url;
//    }
//
//    public void setUrl(String url) {
//        this.url = url;
//    }
//
//    public String getUser() {
//        return user;
//    }
//
//    public void setUser(String user) {
//        this.user = user;
//    }
//
//    public String getPassword() {
//        return password;
//    }
//
//    public void setPassword(String password) {
//        this.password = password;
//    }
//}
