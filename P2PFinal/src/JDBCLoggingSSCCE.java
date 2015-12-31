import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.logging.FileHandler;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public class JDBCLoggingSSCCE {
	private static final String LOG_FILE_NAME = "jdbcloggingsscce.log";
    private static final String DB_URL = "jdbc:mysql://192.168.225.109:3306/wso2devregdb";
    private static final String USER = "devdbusr";
    private static final String PASSWORD = "devdbusr";
    
    private static void initLogger() throws IOException {
        Handler handler = new FileHandler(JDBCLoggingSSCCE.LOG_FILE_NAME);
        handler.setFormatter(new SimpleFormatter());

        Logger logger = Logger.getLogger("");
        logger.setLevel(Level.ALL);
        logger.addHandler(handler);
    }
    
    public static void main(String[] args) throws IOException, SQLException {
        JDBCLoggingSSCCE.initLogger();
        Logger logger = Logger.getLogger(JDBCLoggingSSCCE.class.getName());
        logger.log(Level.INFO, "Starting JDBCLoggingSSCCE");

        Connection conn = DriverManager.getConnection(DB_URL,USER,PASSWORD);
        logger.log(Level.INFO, "JDBC Connection created");
    }

    
    
}