import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

public final class Utility {
	public static Connection con;
	public static String logDbUid;
	public static String logDbPwd;
	public static String logDbURL;
	public static String logLocation;
	public static Logger logger = Logger.getLogger("AppLog");
	public static int runID;
	
	private Utility() {
		
	}
	
	
	public static void intializeLogger() {
		
		
		FileHandler fh;
		try {
			fh = new FileHandler(logLocation + File.separator
					+ "App.log", true);
			logger.addHandler(fh);
			SimpleFormatter formatter = new SimpleFormatter();
			fh.setFormatter(formatter);
			
		} catch (SecurityException e) {
			
			e.printStackTrace();
		} catch (IOException e) {
			
			e.printStackTrace();
		}
		
	}
	
	public static void applicationStart() {
		
		Properties properties = new Properties();
		File pf = new File("config.properties");
		
		try {
			properties.load(new FileReader(pf));
			createConnection(properties.getProperty("LogDBURL"), properties.getProperty("LogDBUID"), properties.getProperty("LogDBPwd"));
			
		} catch (FileNotFoundException e) {
			System.out.println("FileNotFound exception" + e.getMessage());
			System.exit(0);
		} catch (IOException e) {
			System.out.println("IO exception" + e.getMessage());
			System.exit(0);
		}
		
		
		
	}
	
	public static int getRunId() {
		
		String logSql = "";
		
		
	
		return 0;		
				
	}
	public static Connection createConnection(String url,String uid,String pwd) {
		
	
		try {
			
			// Loading the driver
			Class.forName("com.mysql.jdbc.Driver");
			// Creating a connection
			logDbURL = url;
			logDbUid = uid;
			logDbPwd = pwd;
			con = DriverManager.getConnection(logDbURL, logDbUid, logDbPwd);
			
		} catch(ClassNotFoundException e) {
			System.out.println("Driver not found");	
		} catch (SQLException sq1ex) {
			
			System.out.println("Connection exception" + sq1ex.getMessage());
			System.exit(0);
			
		} 
		
		return con;
	}
	
	
	
	
	
	public static void writeLog(int runID,String msg, String type, String entity, String stage, String appender) throws /*SecurityException, */ IOException{

		String logSql = "";
		PreparedStatement ps =  null;

		if(appender.equals("file")) {
			
					
			switch (type) {
			case "info":
				try {
					logger.info(msg);
				} catch (Exception e) {
					e.printStackTrace();
				}
				return;
			case "error":
				try {
					FileHandler fh = new FileHandler(logLocation + File.separator
							+ "App.log", true);
					logger.addHandler(fh);
					SimpleFormatter formatter = new SimpleFormatter();
					fh.setFormatter(formatter);
					logger.severe(msg);
					fh.close();
								

				} catch (Exception e) {
					e.printStackTrace();
				}
				return;
			}
		} else {

			try {
				
				Calendar calendar = Calendar.getInstance();
		    	Timestamp currentTimestamp = new java.sql.Timestamp(calendar.getTime().getTime());
		    	
		    	logSql = "INSERT INTO message_log(runid,message_desc,target_table,message_stage,message_type,message_timestamp) "
						+ "VALUES(?,?,?,?,?,?)";
		    	ps = con.prepareStatement(logSql);
				ps.setInt(1, runID);
		    	ps.setString(2,msg);
				ps.setString(3,entity);
				ps.setString(4,stage);
				ps.setString(5,type);
				ps.setTimestamp(6, currentTimestamp);
				ps.executeUpdate();
				closeConnection(con,ps);
				
				
			} catch (SQLException e1) { //Modified to capture if MySql is down
				//e1.printStackTrace();
				FileHandler fh = new FileHandler(logLocation + File.separator+ "App.log", true); // can throw IOException
				logger.addHandler(fh);
				SimpleFormatter formatter = new SimpleFormatter();
				fh.setFormatter(formatter);
				logger.severe("Error in writing message_log!!  Hence terminating the program. "+e1.getMessage());
				fh.close();
				closeConnection(con,ps);
				System.exit(0);
			}
		}

	}
	
	
	public static void closeConnection(Connection con,PreparedStatement ps) {
		try {
			if (ps != null)
				ps.close();
		} catch (Exception ex) {
		}// nothing we can do
		try {
			if (con != null)
				con.close();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}
		    
	
}