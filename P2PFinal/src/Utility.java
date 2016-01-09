import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Base64;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

public final class Utility {
	private static Connection con;
	private static PreparedStatement ps;
	public static String logDbUid;
	public static String logDbPwd;
	public static String logDbURL;
	public static String logLocation;
	public static Logger logger = Logger.getLogger("AppLog");
	public static int runID;
	public static Map<String, Long> dbObjectJobIdMap = new HashMap<>();
	public static Map<String, String> encConfig = new HashMap<>();
	private Utility() {

	}

	public static void intializeLogger() {

		FileHandler fh;
		try {
			fh = new FileHandler(logLocation + File.separator + "App.log", true);
			logger.addHandler(fh);
			SimpleFormatter formatter = new SimpleFormatter();
			fh.setFormatter(formatter);

		} catch (SecurityException e) {

			e.printStackTrace();
		} catch (IOException e) {

			e.printStackTrace();
		}

	}

	public static void applicationStart(boolean isManual) {

		Properties properties = new Properties();
		File pf = new File("config.properties");

		try {
			properties.load(new FileReader(pf));
			EncryptionUtil.cipher = Cipher.getInstance("AES");

			logDbURL = properties.getProperty("LogDBURL");
			logDbUid = properties.getProperty("LogDBUID");
			logDbPwd = properties.getProperty("LogDBPwd");

			con = createConnection(logDbURL, logDbUid, logDbPwd);

			runID = getRunId(isManual);
			if (!isManual) {

				setRunId(runID++);
			}
			
			

		} catch (FileNotFoundException e) {
			System.out.println("FileNotFound exception" + e.getMessage());
			System.exit(0);
		} catch (IOException e) {
			System.out.println("IO exception" + e.getMessage());
			System.exit(0);
		} catch (NoSuchAlgorithmException e) {

			e.printStackTrace();
		} catch (NoSuchPaddingException e) {

			e.printStackTrace();
		}

	}

	public static int getRunId(boolean isManual) {

		if (isManual)
			return runID;

		String logSql = "SELECT value FROM p2p_config WHERE property = 'RUNID'";
		ResultSet rs = null;

		try {
			if (con.isClosed()) {
				con = createConnection(logDbURL, logDbUid, logDbPwd);
			}
			ps = con.prepareStatement(logSql);
			rs = ps.executeQuery();

			return rs.getInt(0);

		} catch (SQLException e) {

			e.printStackTrace();

		} catch (Exception e) {

			e.printStackTrace();

		} finally {

			closeConnection(con);
		}

		return 1;

	}

	public static void setRunId(int runID) {

		ResultSet rs = null;
		String logSql = null;

		try {

			if (con.isClosed()) {
				con = createConnection(logDbURL, logDbUid, logDbPwd);
			}

			logSql = "UPDATE p2p_config SET value = ? WHERE property = 'RUNID'";
			ps = con.prepareStatement(logSql);
			ps.setString(1, String.valueOf(runID));
			ps.executeUpdate();

		} catch (SQLException e) {

			e.printStackTrace();

		} catch (Exception e) {

			e.printStackTrace();

		} finally {

			closeConnection(con);
		}

	}

	public static Connection createConnection(String url, String uid, String pwd) {

		Connection conn = null;
		try {

			// Loading the driver
			Class.forName("com.mysql.jdbc.Driver");
			// Creating a connection

			logDbURL = url;
			logDbUid = uid;
			logDbPwd = pwd;
			conn = DriverManager.getConnection(logDbURL, logDbUid, logDbPwd);
			return conn;

		} catch (ClassNotFoundException e) {
			System.out.println("Driver not found");
		} catch (SQLException sq1ex) {

			System.out.println("Connection exception" + sq1ex.getMessage());
			System.exit(0);

		}

		return conn;
	}

	public static void writeLog(String msg, String type, String entity,
			String stage, String appender)
			throws /* SecurityException, */IOException {

		String logSql = "";

		if (appender.equals("file")) {

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
					FileHandler fh = new FileHandler("App.log", true);
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

				if (con.isClosed()) {
					con = createConnection(logDbURL, logDbUid, logDbPwd);
				}

				Calendar calendar = Calendar.getInstance();
				Timestamp currentTimestamp = new java.sql.Timestamp(calendar
						.getTime().getTime());

				logSql = "INSERT INTO message_log(runid,message_desc,target_table,message_stage,message_type,message_timestamp) "
						+ "VALUES(?,?,?,?,?,?)";

				ps = con.prepareStatement(logSql);
				ps.setInt(1, Utility.runID);
				ps.setString(2, msg);
				ps.setString(3, entity);
				ps.setString(4, stage);
				ps.setString(5, type);
				ps.setTimestamp(6, currentTimestamp);
				ps.executeUpdate();
				closeConnection(con);

			} catch (SQLException e1) { // Modified to capture if MySql is down
				// e1.printStackTrace();
				FileHandler fh = new FileHandler("App.log", true); // can throw
																	// IOException
				logger.addHandler(fh);
				SimpleFormatter formatter = new SimpleFormatter();
				fh.setFormatter(formatter);
				logger.severe("Error in writing message_log!!  Hence terminating the program. "
						+ e1.getMessage());
				fh.close();
				closeConnection(con);
				System.exit(0);
			}
		}

	}

	public void updateConfig(String property, String value) {

		PreparedStatement ps;
		String logSql = "";

		try {

			if (con.isClosed()) {
				con = createConnection(logDbURL, logDbUid, logDbPwd);
			}

			logSql = "UPDATE p2p_config SET value = ? WHERE key = ?";
			ps = con.prepareStatement(logSql);
			ps.setString(1, value);
			ps.setString(2, property);
			ps.executeUpdate();

		} catch (SQLException e) {

			e.printStackTrace();

		} catch (Exception e) {

			e.printStackTrace();
		} finally {

			closeConnection(con);
		}

	}

	public static String getConfig(String property) {

		String logSql = "";
		ResultSet rs = null;
		byte[] encodedKey     = Base64.decode( encConfig.get("secretKey"), Base64.DEFAULT);
		SecretKey originalKey = new SecretKeySpec(encodedKey, 0, encodedKey.length, "AES");
		try {
			
			if(encConfig.containsKey(property)) {
				
				return EncryptionUtil.decrypt(encConfig.get(property),((SecretKey) encConfig.get("secretKey")));
			} else {
			
				if (con.isClosed()) {
					con = createConnection(logDbURL, logDbUid, logDbPwd);
				}
				logSql = "SELECT value FROM p2p_config WHERE key = ?";
				ps = con.prepareStatement(logSql);
				ps.setString(1, property);
				rs = ps.executeQuery();
	
				return EncryptionUtil.decrypt(rs.getString(0), );
			}

		} catch (SQLException e) {

			e.printStackTrace();
		} catch (Exception e) {

			e.printStackTrace();
		} finally {

			closeConnection(con);
		}

		return "";
	}

	public static long writeJobLog(int runID, String entity, String run_mode,
			String job_status) {

		String logSql = "";
		long key = -1L;

		try {
			if (con.isClosed()) {
				con = createConnection(logDbURL, logDbUid, logDbPwd);
			}

			logSql = "INSERT INTO job_log(runid,entity,run_mode,job_status) "
					+ "VALUES(?,?,?,?)";

			ps = con.prepareStatement(logSql, Statement.RETURN_GENERATED_KEYS);
			ps.setInt(1, runID);
			ps.setString(2, entity);
			ps.setString(3, run_mode);
			ps.setString(4, job_status);
			ps.executeUpdate();
			ResultSet rs = ps.getGeneratedKeys();
			if (rs != null && rs.next()) {
				key = rs.getLong(1);
			}

		} catch (Exception e) {
			e.printStackTrace();

		} finally {

			closeConnection(con);
		}

		return key;
	}

	public static void writeJobLog(long key, String optName, String optTime) {

		String logSql = "";
		try {

			if (con.isClosed()) {
				con = createConnection(logDbURL, logDbUid, logDbPwd);
			}

			switch (optName.toUpperCase()) {

			case "EXTRACTSTART":

				logSql = "UPDATE job_log SET ExtractStart = ? WHERE job_id = ? ";
				break;

			case "EXTRACTEND":

				logSql = "UPDATE job_log SET ExtractEnd = ? WHERE job_id = ? ";
				break;

			case "S3LOADSTART":

				logSql = "UPDATE job_log SET S3LoadStart = ? WHERE job_id = ? ";
				break;

			case "S3LOADEND":

				logSql = "UPDATE job_log SET S3LoadEnd = ? WHERE job_id = ? ";
				break;

			case "REDSHIFTLOADSTART":

				logSql = "UPDATE job_log SET RedShiftLoadStart = ? WHERE job_id = ? ";
				break;

			case "REDSHIFTLOADEND":

				logSql = "UPDATE job_log SET RedShiftLoadEnd = ? WHERE job_id = ? ";
				break;

			case "ERROR":
				logSql = "UPDATE job_log SET job_status = ? WHERE job_id = ? ";
				break;

			case "COMPLETED":
				logSql = "UPDATE job_log SET RedShiftLoadEnd = ?, job_status ='SUCCESS' WHERE job_id = ? ";
				break;

			default:

				logSql = "UPDATE job_log SET job_status = ? WHERE job_id = ? ";
				break;

			}
			if (!optName.equalsIgnoreCase("error")) {
				ps = con.prepareStatement(logSql);
				Timestamp ts = Timestamp.valueOf(optTime);
				ps.setTimestamp(1, ts);
				ps.setInt(2, (int) key);
				ps.executeUpdate();

			} else {
				ps = con.prepareStatement(logSql);
				ps.setString(1, "Error");
				ps.setInt(2, (int) key);
				ps.executeUpdate();
			}

		} catch (SQLException e) {
			e.printStackTrace();

		} catch (Exception e) {
			e.printStackTrace();

		} finally {

			closeConnection(con);
		}
	}

	public static void closeConnection(Connection con) {

		try {
			if (!con.isClosed()) {
				con.close();
				con = null;
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
	}

}