import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileFilter;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.lang.RuntimeException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Date;
import java.util.Scanner;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.logging.FileHandler;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

import org.supercsv.io.CsvMapReader;
import org.supercsv.io.CsvMapWriter;
import org.supercsv.io.ICsvMapReader;
import org.supercsv.io.ICsvMapWriter;
import org.supercsv.prefs.CsvPreference;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.Upload;

public class DataLoader {
	private String appConfigPropFile;
	private String serverHost;
	private String serverDataSource;
	private String login;
	private String password;
	private String accountId;
	private int roleId;
	private int port;
	private String[] dimensions;
	private String[] facts;
	private Connection con;
	private String connectionString;
	private Statement statement;
	private ResultSet resultSet;
	private String query;
	private String extractLocationLocal;
	private String extractLocationURL;
	private String extractLocationS3;
	private String statusRptLocation;
	private String factsSqlScriptLocation;
	private String factsPropFile;
	private TransferManager tx;
	private Upload upload;
	private String logLocation;
	private String awsProfile;
	private ArrayList<String> extractStartTime = new ArrayList<String>();
	private ArrayList<String> extractEndTime = new ArrayList<String>();
	private ArrayList<String> loadStartTime = new ArrayList<String>();
	private ArrayList<String> loadEndTime = new ArrayList<String>();
	private ArrayList<String> loadStartTimeRS = new ArrayList<String>();
	private ArrayList<String> loadEndTimeRS = new ArrayList<String>();
	private String redShiftMasterUsername;
	private String redShiftMasterUserPassword;
	private String redShiftPreStageSchemaName;
	private String dbURL;
	private Map<String, String> checkList = new HashMap<String, String>();
	private char csvDelimiter;
	private int RunID;
	private String eol;
	private String aSQLScriptFilePath;
	private File tmpLog;
	private String ExtractURL;
	private String logDbURL;
	private String logDbUid;
	private String logDbPwd;
	private int[][] timeArr;
	private long[] insertID;
	private final String[] DIM;

	public DataLoader() throws IOException, ClassNotFoundException,
			SQLException {
		appConfigPropFile = "config.properties";
		Properties properties = new Properties();
		File pf = new File(appConfigPropFile);
		properties.load(new FileReader(pf));

		factsPropFile = "facts.properties";
		eol = System.getProperty("line.separator");

		serverHost = properties.getProperty("ServerHost");
		serverDataSource = properties.getProperty("DataSource");
		login = properties.getProperty("Login");
		password = properties.getProperty("Password");
		accountId = properties.getProperty("AccountId");
		roleId = convertToNumber(properties.getProperty("RoleId"), "RoleID");
		port = convertToNumber(properties.getProperty("Port"), "Port");

		if (!properties.getProperty("Dimensions").equalsIgnoreCase("NONE")) {
			dimensions = properties.getProperty("Dimensions").split(",");
		} else {
			dimensions = null;
		}

		if (!properties.getProperty("Facts").equalsIgnoreCase("NONE")) {
			facts = properties.getProperty("Facts").split(",");
		} else {
			facts = null;
		}

		SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");
		Date curDate = new Date();
		String strDate = sdf.format(curDate);

		extractLocationLocal = System.getProperty("user.dir") + File.separator
				+ "DB_Extracts" + File.separator + strDate;
		extractLocationURL = System.getProperty("user.dir") + File.separator
				+ "URL_DB_Extracts" + File.separator + strDate;
		extractLocationS3 = properties.getProperty("s3bucket");
		logLocation = System.getProperty("user.dir") + File.separator + "log"
				+ File.separator + strDate;
		statusRptLocation = System.getProperty("user.dir") + File.separator
				+ "Status_Reports" + File.separator + strDate;
		factsSqlScriptLocation = properties.getProperty("FactFileLoc");

		aSQLScriptFilePath = properties.getProperty("SQLScriptsPath");
		awsProfile = properties.getProperty("AwsProfile");
		redShiftMasterUsername = properties.getProperty("RSUID");
		redShiftMasterUserPassword = properties.getProperty("RSPWD");
		redShiftPreStageSchemaName = properties.getProperty("RSSCHEMAPRESTAGE");
		dbURL = properties.getProperty("RSDBURL");
		csvDelimiter = properties.getProperty("CSVDelim").charAt(0);
		ExtractURL = properties.getProperty("FileExtractURL");
		RunID = Integer.parseInt(properties.getProperty("RunID"));
		RunID++;
		updateFactsProperty(appConfigPropFile, "RunID ", String.valueOf(RunID));
		logDbURL = properties.getProperty("LogDBURL");
		logDbUid = properties.getProperty("LogDBUID");
		logDbPwd = properties.getProperty("LogDBPwd");
		int cnt = dimensions.length + (facts[0] == "NONE" ? 0 : facts.length);
		timeArr = new int[6][cnt];
		DIM = properties.getProperty("Dimensions1").split(",");

		try {
			new File(extractLocationLocal).mkdirs();
			new File(extractLocationURL).mkdirs();
			new File(logLocation).mkdirs();
			new File(statusRptLocation).mkdirs();

			// Creating JDBC DB Connection
			Class.forName("com.netsuite.jdbc.openaccess.OpenAccessDriver");
			connectionString = String
					.format("jdbc:ns://%s:%d;ServerDataSource=%s;encrypted=1;CustomProperties=(AccountID=%s;RoleID=%d)",
							serverHost, port, serverDataSource, accountId,
							roleId);

			con = DriverManager
					.getConnection(connectionString, login, password);

			writeLog(
					"Application Started Successfully.RunID  of this session is "
							+ RunID, "info", "", "Aplication Startup", "db");

			System.out
					.println("************************************ WELCOME TO P2P DB DATA LOADER UTILITIES ************************************");

		} catch (SecurityException e) {
			System.out
					.println("Application Can't create Log Directory. See Error Message for mor details."
							+ e.getMessage());

			tmpLog = new File("tmp.log");
			FileWriter fWriter = new FileWriter(tmpLog);
			PrintWriter pWriter = new PrintWriter(fWriter);
			pWriter.println("Application Can't create Log Directory. See Error Message for mor details."
					+ e.getMessage());

			System.exit(0);

		} catch (ClassNotFoundException e) {
			System.out.println("Error !! Please check error message "
					+ e.getMessage());
			writeLog("RunID " + RunID + "Error !! Please check error message. "
					+ e.getMessage(), "error", "", "Aplication Startup", "db");
			System.exit(0);
		} catch (SQLException se) {
			System.out.println("Error !! Please check error message "
					+ se.getMessage());
			writeLog("RunID " + RunID + "Error !! Please check error message. " + se.getMessage(),
					"error","","Aplication Startup","db");
			System.exit(0);
		}

	}

	public DataLoader(String mode, int runid) throws IOException,
			ClassNotFoundException, SQLException {
		appConfigPropFile = "config.properties";
		Properties properties = new Properties();
		File pf = new File(appConfigPropFile);
		properties.load(new FileReader(pf));

		factsPropFile = "facts.properties";
		eol = System.getProperty("line.separator");

		// Initialize the class variables with properties values
		serverHost = properties.getProperty("ServerHost");
		serverDataSource = properties.getProperty("DataSource");
		login = properties.getProperty("Login");
		password = properties.getProperty("Password");
		accountId = properties.getProperty("AccountId");
		roleId = convertToNumber(properties.getProperty("RoleId"), "RoleID");
		port = convertToNumber(properties.getProperty("Port"), "Port");

		if (!properties.getProperty("Dimensions").equalsIgnoreCase("NONE")) {
			dimensions = properties.getProperty("Dimensions").split(",");
		} else {
			dimensions = null;
		}

		if (!properties.getProperty("Facts").equalsIgnoreCase("NONE")) {
			facts = properties.getProperty("Facts").split(",");
		} else {
			facts = null;
		}

		SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");
		Date curDate = new Date();
		String strDate = sdf.format(curDate);

		extractLocationLocal = System.getProperty("user.dir") + File.separator
				+ "DB_Extracts" + File.separator + strDate;
		extractLocationS3 = properties.getProperty("s3bucket");
		logLocation = System.getProperty("user.dir") + File.separator + "log"
				+ File.separator + strDate;
		statusRptLocation = System.getProperty("user.dir") + File.separator
				+ "Status_Reports" + File.separator + strDate;
		factsSqlScriptLocation = System.getProperty("user.dir")
				+ File.separator + properties.getProperty("FactFileLoc");

		aSQLScriptFilePath = properties.getProperty("SQLScriptsPath");
		awsProfile = properties.getProperty("AwsProfile");
		redShiftMasterUsername = properties.getProperty("RSUID");
		redShiftMasterUserPassword = properties.getProperty("RSPWD");
		redShiftPreStageSchemaName = properties.getProperty("RSSCHEMAPRESTAGE");
		dbURL = properties.getProperty("RSDBURL");
		csvDelimiter = properties.getProperty("CSVDelim").charAt(0);
		RunID = runid;
		DIM = properties.getProperty("Dimensions1").split(",");
		logDbURL = properties.getProperty("LogDBURL");
		logDbUid = properties.getProperty("LogDBUID");
		logDbPwd = properties.getProperty("LogDBPwd");
		
		try {
			new File(extractLocationLocal).mkdirs();
			new File(logLocation).mkdirs();
			new File(statusRptLocation).mkdirs();
			// Creating JDBC DB Connection
			Class.forName("com.netsuite.jdbc.openaccess.OpenAccessDriver");
			connectionString = String
					.format("jdbc:ns://%s:%d;ServerDataSource=%s;encrypted=1;CustomProperties=(AccountID=%s;RoleID=%d)",
							serverHost, port, serverDataSource, accountId,
							roleId);

			con = DriverManager
					.getConnection(connectionString, login, password);

			writeLog(
					"Application Started Successfully.RunID  of this session is "
							+ RunID, "info", "", "Aplication Startup", "db");

			System.out
					.println("************************************ WELCOME TO P2P DB DATA LOADER UTILITIES ************************************");

		} catch (SecurityException e) {
			System.out
					.println("Application Can't create Log Directory. See Error Message for mor details."
							+ e.getMessage());

			tmpLog = new File("tmp.log");
			FileWriter fWriter = new FileWriter(tmpLog);
			PrintWriter pWriter = new PrintWriter(fWriter);
			pWriter.println("Application Can't create Log Directory. See Error Message for mor details."
					+ e.getMessage());

			System.exit(0);

		} catch (ClassNotFoundException e) {
			System.out.println("Error !! Please check error message "
					+ e.getMessage());
			writeLog("RunID " + RunID + "Error !! Please check error message. "
					+ e.getMessage(), "error", "", "Aplication Startup", "db");
			System.exit(0);
		}

	}

	public int getRunID() {
		return this.RunID;
	}

	public void setRunID() {

		RunID++;

	}

	public void createDbExtract() throws IOException {

		String TBL_DEF_CONF = "tbl_def.properties";
		String clmNames;
		String factFileName;
		int count;
		String ts;

		Properties properties = new Properties();
		File pf = new File(TBL_DEF_CONF);
		properties.load(new FileReader(pf));

		if (dimensions != null) {
			for (int i = 0; i < dimensions.length; i++) {

				System.out
						.println("DataExtraction Operation Started for DB table "
								+ dimensions[i]);
				System.out
						.println("--------------------------------------------------------");

				writeLog("RunID " + RunID
						+ " DataExtraction Operation Started for DB table "
						+ dimensions[i], "info", dimensions[i],
						"DataExtraction", "db");
				

					insertID[i] = writeJobLog(getRunID(), dimensions[i],
							"Normal", "In-Progress");

				

				// LOAD USER SPECIFIC COLUMNS FROM THE TABLE DEFINED IN
				// PROPERTIES FILE

				query = properties.getProperty(dimensions[i]);

				try {

					writeJobLog(insertID[i], "EXTRACTSTART",
							new SimpleDateFormat("HH:mm:ss").format(Calendar
									.getInstance().getTime()));

					writeLog("RunID " + RunID + " Retrieving data for "
							+ dimensions[i], "info", dimensions[i],
							"DataExtraction", "db");

					System.out.println("Retrieving data...");

					statement = con.createStatement();

					System.out.println("Executing query for " + dimensions[i]
							+ " table.");

					resultSet = statement.executeQuery(query);

					SimpleDateFormat sdf = new SimpleDateFormat(
							"ddMMyyyyhhmmss");
					Date curDate = new Date();
					String strDate = sdf.format(curDate);

					String fileName = extractLocationLocal + File.separator
							+ dimensions[i] + "-" + "RunID-" + RunID + "-"
							+ strDate + ".csv";
					File file = new File(fileName);
					FileWriter fstream = new FileWriter(file);
					BufferedWriter out = new BufferedWriter(fstream);
					String str = "";
					List<String> columnNames = getColumnNames(resultSet);
					for (int c = 0; c < columnNames.size(); c++) {
						str = "\"" + columnNames.get(c) + "\"";
						out.append(str);
						if (c != columnNames.size() - 1) {
							out.append(csvDelimiter);
						}
					}

					// process results

					while (resultSet.next()) {

						List<Object> row = new ArrayList<Object>();

						row.add(RunID);

						for (int k = 0; k < columnNames.size() - 1; k++) {
							row.add(resultSet.getObject(k + 1));

						}

						out.newLine();

						for (int j = 0; j < row.size(); j++) {
							if (!String.valueOf(row.get(j)).equals("null")) {
								String tmp = "\"" + String.valueOf(row.get(j))
										+ "\"";
								out.append(tmp);
								if (j != row.size() - 1) {
									out.append(csvDelimiter);
								}
							} else {
								if (j != row.size() - 1) {
									out.append(csvDelimiter);
								}
							}
						}
					}
					out.close();

					writeJobLog(insertID[i], "EXTRACTEND",
							new SimpleDateFormat("HH:mm:ss").format(Calendar
									.getInstance().getTime()));

					File csvFile = new File(fileName);
					double bytes = csvFile.length();

					writeLog("RunID  No." + RunID
							+ " DataExtration Oparation for table "
							+ dimensions[i] + " is extracted in " + fileName,
							"info", dimensions[i], "Dataextraction", "db");

					System.out.println("DataExtration Oparation for table "
							+ dimensions[i] + " is extracted in " + fileName);

					checkList.put(dimensions[i], "Extraction Done");

					extractEndTime.add(i, new SimpleDateFormat("HH:mm:ss")
							.format(Calendar.getInstance().getTime()));

				} catch (SQLException e) {
					System.out
							.println("SQL Error!! Please Cheack the query and try again.\n"
									+ e.getMessage());

					checkList.put(dimensions[i], "Extraction Error");

					writeLog(
							"RunID " + RunID + " SQL Error!!" + e.getMessage(),
							"error", dimensions[i], "Dataextraction", "db");

					extractEndTime.add(i, "Error");

					writeJobLog(insertID[i], "Error", "");

				} catch (IOException e) {

					System.out
							.println("IO Error!! Please check the error message.\n"
									+ e.getMessage());

					checkList.put(dimensions[i], "Extraction Error");

					writeLog("RunID " + RunID + " Error!!" + e.getMessage(),
							"error", dimensions[i], "Dataextraction", "db");
					extractEndTime.add(i, "Error");

					writeJobLog(insertID[i], "Error", "");

				} catch (Exception e) {
					System.out.println("RunID " + RunID
							+ " Error!! Please check the error message.\n"
							+ e.getMessage());

					checkList.put(dimensions[i], "Extraction Error");

					writeLog("RunID " + RunID + " Error!!" + e.getMessage(),
							"error", dimensions[i], "Dataextraction", "db");
					extractEndTime.add(i, "Error");

					writeJobLog(insertID[i], "Error", "");

				} finally {
					continue;
				}

			}
		} else {

			System.out.println("No dimentions are mentioned in the config file. Facts processing cannot be done without dimension, hence terminating the program.");
			writeLog("RunID " + RunID + "Error !! No dimentions are mentioned in the config file. Facts processing cannot be done without dimension, hence terminating the program.",
					"error","","DataExtraction","db");
			System.exit(0);
		}

		/***************************** Fact Extraction Started **************************************/

		if (facts != null) {
			int idex = dimensions.length - 1;
			System.out.println("DataExtration Oparation is Started for Facts.");

			writeLog("RunID " + RunID
					+ " DataExtration Oparation is Started for Facts.", "info",
					"", "Dataextraction", "db");
			int cunt1 = insertID.length;
			for (int x = 0; x < facts.length; x++) {

				

					insertID[cunt1] = writeJobLog(getRunID(), facts[x],
							"Normal", "In-Progress");
					
					writeJobLog(insertID[cunt1], "EXTRACTSTART",
							new SimpleDateFormat("HH:mm:ss").format(Calendar
									.getInstance().getTime()));

				

					
				
				idex = idex + x + 1;
				extractStartTime.add(idex, new SimpleDateFormat("HH:mm:ss")
						.format(Calendar.getInstance().getTime()));

				try {

					factFileName = factsSqlScriptLocation + File.separator
							+ facts[x] + ".sql";

					createFactExtract(con, factFileName, facts[x], idex);
					
					writeJobLog(insertID[cunt1], "EXTRACTEND",
							new SimpleDateFormat("HH:mm:ss").format(Calendar
									.getInstance().getTime()));

				} catch (Exception e) {

					extractEndTime.add(idex, "Error");

					checkList.put(facts[x], "Extraction Error");

					writeLog("RunID " + RunID + " Error!!" + e.getMessage(),
							"error", facts[x], "Dataextraction", "db");
					
					writeJobLog(insertID[cunt1], "Error","");
					

				} finally {
					continue;
				}
			}
		} else {
			System.out.println("No facts are mentioned in the config file.");
		}

	}

	public void createDbExtract(char mode, String[] failedFileList)
			throws IOException {

		String TBL_DEF_CONF = "tbl_def.properties";
		String clmNames;
		String factFileName;

		for (int i = 0; i < failedFileList.length; i++) {

			if (Arrays.asList(DIM).contains(failedFileList[i])) {

				System.out
						.println("DataExtraction Operation Started for DB table "
								+ failedFileList[i]);
				System.out
						.println("--------------------------------------------------------");

				writeLog("DataExtraction Operation Started for DB table "
						+ failedFileList[i], "info", failedFileList[i],
						"Dataextraction_Reprocess", "db");

				// LOAD USER SPECIFIC COLUMNS FROM THE TABLE DEFINED IN
				// PROPERTIES FILE

				Properties properties = new Properties();
				File pf = new File(TBL_DEF_CONF);
				properties.load(new FileReader(pf));

				clmNames = properties.getProperty(failedFileList[i]);

				if (clmNames.equalsIgnoreCase("ALL"))
					query = "SELECT * FROM " + failedFileList[i] + ";";
				else
					query = "SELECT " + clmNames + " FROM " + failedFileList[i]
							+ ";";

				try {

					insertID[i] = writeJobLog(getRunID(), failedFileList[i],
							"Re-Process", "In-Progress");
					
					writeJobLog(insertID[i], "EXTRACTSTART",
							new SimpleDateFormat("HH:mm:ss").format(Calendar
									.getInstance().getTime()));

					statement = con.createStatement();
					System.out.println("Executing query for "
							+ failedFileList[i] + " table.");

					resultSet = statement.executeQuery(query);

					System.out.println("Retrieving data...");

					writeLog("Retrieving data for " + failedFileList[i],
							"info", failedFileList[i],
							"Dataextraction_Reprocess", "db");

					SimpleDateFormat sdf = new SimpleDateFormat(
							"ddMMyyyyhhmmss");
					Date curDate = new Date();
					String strDate = sdf.format(curDate);

					String fileName = extractLocationLocal + File.separator
							+ failedFileList[i] + "-" + "RunID-" + RunID + "-"
							+ strDate + ".csv";
					File file = new File(fileName);
					FileWriter fstream = new FileWriter(file);
					BufferedWriter out = new BufferedWriter(fstream);
					String str = "";
					List<String> columnNames = getColumnNames(resultSet);

					for (int c = 0; c < columnNames.size(); c++) {
						str = "\"" + columnNames.get(c) + "\"";
						out.append(str);
						if (c != columnNames.size() - 1) {
							out.append(",");
						}
					}

					// process results
					while (resultSet.next()) {
						List<Object> row = new ArrayList<Object>();
						// do something meaning full with the result
						for (int k = 0; k < columnNames.size(); k++) {
							row.add(resultSet.getObject(k + 1));
						}

						out.newLine();

						for (int j = 0; j < row.size(); j++) {
							if (!String.valueOf(row.get(j)).equals("null")) {
								String tmp = "\"" + String.valueOf(row.get(j))
										+ "\"";
								out.append(tmp);
								if (j != row.size() - 1) {
									out.append(",");
								}
							} else {
								if (j != row.size() - 1) {
									out.append(",");
								}
							}
						}
					}
					out.close();
					
					writeJobLog(insertID[i], "EXTRACTEND",
							new SimpleDateFormat("HH:mm:ss").format(Calendar
									.getInstance().getTime()));
					
					writeLog("RunID " + RunID
							+ " DataExtration Oparation for table "
							+ failedFileList[i] + " is extracted in "
							+ fileName, "info", failedFileList[i],
							"Dataextraction_Reprocess", "db");
					
					System.out.println("DataExtration Oparation for table "
							+ failedFileList[i] + " is extracted in "
							+ fileName);

					

				} catch (SQLException e) {
					System.out
							.println("SQL Error!! Please Cheack the query and try again.\n"
									+ e.getMessage());

					// checkList.put(dimensions[i],"Extraction Error");

					writeLog(
							"RunID " + RunID + " SQL Error!!" + e.getMessage(),
							"error", failedFileList[i], "Dataextraction", "DB");
					
					writeJobLog(insertID[i], "Error","");

					// extractEndTime.add(i, "Error");

				} catch (IOException e) {
					
					System.out
							.println("IO Error!! Please check the error message.\n"
									+ e.getMessage());

					// checkList.put(dimensions[i],"Extraction Error");

					writeLog("RunID " + RunID + " Error!!" + e.getMessage(),
							"error", failedFileList[i], "Dataextraction", "DB");
					
					writeJobLog(insertID[i], "Error","");
					// extractEndTime.add(i, "Error");

				} catch (Exception e) {
					System.out
							.println("Error!! Please check the error message.\n"
									+ e.getMessage());

					// checkList.put(dimensions[i],"Extraction Error");

					writeLog("RunID " + RunID + " Error!!" + e.getMessage(),
							"error", failedFileList[i], "Dataextraction", "DB");
					
					writeJobLog(insertID[i], "Error","");
					// extractEndTime.add(i, "Error");

				} finally {
					continue;
				}

			} else {

				try {

					factFileName = factsSqlScriptLocation + File.separator
							+ failedFileList[i] + ".sql";
					
					writeLog("DataExtraction Operation Started for DB table "
							+ failedFileList[i], "info", failedFileList[i],
							"Dataextraction_Reprocess", "db");

					insertID[i] = writeJobLog(getRunID(), failedFileList[i],
							"Re-Process", "In-Progress");
					
					writeJobLog(insertID[i], "EXTRACTSTART",
							new SimpleDateFormat("HH:mm:ss").format(Calendar
									.getInstance().getTime()));
					
					createFactExtract(con, factFileName, failedFileList[i], -1);
					
					writeJobLog(insertID[i], "EXTRACTSTART",
							new SimpleDateFormat("HH:mm:ss").format(Calendar
									.getInstance().getTime()));
					
					writeLog("DataExtraction Operation Ended for DB table "
							+ failedFileList[i], "info", failedFileList[i],
							"Dataextraction_Reprocess", "db");

				} catch (Exception e) {

					// extractEndTime.add(idex, "Error");

					// checkList.put(facts[x],"Extraction Error");
					
					writeJobLog(insertID[i], "Error","");

					writeLog("RunID " + RunID + " Error!!" + e.getMessage(),
							"error", failedFileList[i], "DataExtraction", "DB");

				} finally {
					continue;
				}
			}

		}

	}

	private void createFactExtract(Connection conn, String factFile,
			String factName, int pos) throws SQLException, IOException {

		Statement st = null;
		ResultSet rs = null;
		String lastModDate;
		String[] tmpdt = null;
		String currDate = null;

		Properties factsProp = new Properties();
		Properties tmpProp = new Properties();
		File pf = new File(factsPropFile);
		File tmpFile = new File("tmpFile.properties");
		tmpFile.createNewFile();

		try {
			factsProp.load(new FileReader(pf));
			tmpProp.load(new FileReader(tmpFile));

		} catch (IOException e) {

			throw new IOException(e.toString());
		}

		try {
			BufferedReader in = new BufferedReader(new FileReader(factFile));

			Scanner scn = new Scanner(in);
			scn.useDelimiter("/\\*[\\s\\S]*?\\*/|--[^\\r\\n]*|;");

			st = conn.createStatement();

			while (scn.hasNext()) {
				String line = scn.next().trim();

				if (!line.isEmpty()) {
					tmpdt = factsProp.getProperty(factName).split(",");
					SimpleDateFormat formatter = new SimpleDateFormat(
							"yyyy-MM-dd HH:mm:ss");
					Date date = formatter.parse(tmpdt[1]);
					Calendar cal = Calendar.getInstance();
					cal.setTime(date);
					cal.add(Calendar.SECOND, 1);
					lastModDate = cal.getTime().toString();
					line = String.format(line, lastModDate);
					writeLog("RunID " + RunID + " Executing query for "
							+ factName + "where DATE_LAST_MODIFIED >= "
							+ lastModDate, "info", factName, "Dataextraction",
							"DB");

					rs = st.executeQuery(line);

					currDate = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
							.format(Calendar.getInstance().getTime());
					lastModDate = tmpdt[1] + "," + currDate;
					updateFactsProperty("tmpFile.properties", factName,
							lastModDate);

					if (rs.next()) {

						System.out.println("Retrieving data...");

						writeLog("Retrieving data for " + factName, "info",
								factName, "Dataextraction", "DB");

						SimpleDateFormat sdf = new SimpleDateFormat(
								"ddMMyyyyhhmmss");
						Date curDate = new Date();
						String strDate = sdf.format(curDate);

						String fileName = extractLocationLocal + File.separator
								+ factName + "-" + "RunID-" + RunID + "-"
								+ strDate + ".csv";
						File file = new File(fileName);
						FileWriter fstream = new FileWriter(file);
						BufferedWriter out = new BufferedWriter(fstream);

						String str = "";
						List<String> columnNames = getColumnNames(rs);
						for (int c = 0; c < columnNames.size(); c++) {
							str = "\"" + columnNames.get(c) + "\"";
							out.append(str);
							if (c != columnNames.size() - 1) {
								out.append(csvDelimiter);
							}
						}

						while (rs.next()) {
							List<Object> row = new ArrayList<Object>();

							for (int k = 0; k < columnNames.size(); k++) {
								row.add(rs.getObject(k + 1));
							}

							out.newLine();

							for (int j = 0; j < row.size(); j++) {
								if (!String.valueOf(row.get(j)).equals("null")) {
									String tmp = "\""
											+ String.valueOf(row.get(j)) + "\"";
									out.append(tmp);
									if (j != row.size() - 1) {
										out.append(csvDelimiter);
									}
								} else {
									if (j != row.size() - 1) {
										out.append(csvDelimiter);
									}
								}
							}
						}
						out.close();
						fstream.close();

						if (pos >= 0) {
							checkList.put(factName, "Extraction Done");
							extractEndTime.add(pos, new SimpleDateFormat(
									"HH:mm:ss").format(Calendar.getInstance()
									.getTime()));
						}

						System.out.println("Extraction Completed for fact "
								+ factName);
						writeLog("RunID " + RunID
								+ " Extraction Completed for fact " + factName,
								"info", factName, "Dataextraction", "DB");
					} else {
						System.out
								.println("No resultset generated after executing query for "
										+ factName
										+ "where DATE_LAST_MODIFIED >= "
										+ lastModDate);
						writeLog(
								"RunID "
										+ RunID
										+ " No resultset generated after executing query for "
										+ factName
										+ "where DATE_LAST_MODIFIED >= "
										+ lastModDate, "info", factName,
								"Dataextraction", "DB");
						if (pos >= 0) {
							checkList.put(factName, "Extraction Error");
							extractEndTime.add(pos, "Error");
						}
						scn.close();

						if (st != null)
							st.close();
						return;

					}

				}

			}
			scn.close();
			if (st != null)
				st.close();

		} catch (FileNotFoundException e) {

			writeLog("RunID " + RunID + " Error!!" + e.getMessage(), "error",
					"", "Dataextraction", "DB");
			throw new FileNotFoundException(e.toString());

		} catch (SQLException e) {

			writeLog("RunID " + RunID + " Error!!" + e.getMessage(), "error",
					"", "Dataextraction", "DB");
			throw new SQLException();

		} catch (IOException e) {

			writeLog("RunID " + RunID + " Error!!" + e.getMessage(), "error",
					"", "Dataextraction", "DB");
			throw new IOException(e.toString());

		} catch (ParseException e) {
			writeLog("RunID " + RunID + " Error!!" + e.getMessage(), "error",
					"", "Dataextraction", "DB");
			throw new SQLException(e.toString());
		}

	}

	public void doAmazonS3FileTransfer() throws SecurityException, IOException {

		String[] files = combine(dimensions, facts);
		AWSCredentials credentials = null;
		try {

			credentials = new ProfileCredentialsProvider(awsProfile)
					.getCredentials();
		} catch (Exception e) {
			writeLog(
					"Cannot load the credentials from the credential profiles file. "
							+ "Please make sure that your credentials file is at the correct "
							+ "location, and is in valid format.", "error", "",
					"S3Transfer", "DB");
			throw new AmazonClientException(
					"Cannot load the credentials from the credential profiles file. "
							+ "Please make sure that your credentials file is at the correct "
							+ "location, and is in valid format.", e);

		}

		AmazonS3 s3 = new AmazonS3Client(credentials);
		Region usWest2 = Region.getRegion(Regions.US_EAST_1);
		s3.setRegion(usWest2);
		tx = new TransferManager(s3);

		SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");
		Date curDate = new Date();
		String strDate = sdf.format(curDate);

		String bucketName = extractLocationS3;
		String key = credentials.getAWSAccessKeyId();

		System.out.println("\n\nUploading DB Extracts to Amazon S3");
		System.out
				.println("--------------------------------------------------------");

		for (int i = 0; i < files.length; i++) {

			if (!checkErrorStatus("Extraction", files[i])) {

				loadStartTime.add(i, new SimpleDateFormat("HH:mm:ss")
						.format(Calendar.getInstance().getTime()));
				
								
				writeJobLog(insertID[i], "S3LOADSTART",
						new SimpleDateFormat("HH:mm:ss").format(Calendar
								.getInstance().getTime()));

				try {

					File s3File = getLastFileModifiedFile(extractLocationLocal,
							files[i]);
					writeLog(
							"RunID " + RunID + " Uploading " + s3File.getName()
									+ " to Amazon S3 bucket " + bucketName,
							"info", files[i], "S3Transfer", "DB");
					System.out.println("Uploading " + s3File.getName()
							+ " to S3.\n");

					PutObjectRequest request = new PutObjectRequest(bucketName,
							strDate + "/" + s3File.getName(), s3File);
					upload = tx.upload(request);
					System.out.println("Checking transfer status=="+upload.getState().toString()+upload.isDone());
					upload.waitForCompletion();
					tx.shutdownNow();
					writeJobLog(insertID[i], "S3LOADEND",
							new SimpleDateFormat("HH:mm:ss").format(Calendar
									.getInstance().getTime()));
					
					writeLog("RunID " + RunID + s3File.getName()
							+ " transffered successfully.", "info", files[i],
							"S3Transfer", "DB");
					
					System.out.println(files[i] + " file transffered.");

					loadEndTime.add(i, new SimpleDateFormat("HH:mm:ss")
							.format(Calendar.getInstance().getTime()));
					
					
					writeJobLog(insertID[i], "REDSHIFTLOADSTART",
							new SimpleDateFormat("HH:mm:ss").format(Calendar
									.getInstance().getTime()));
					
					loadDataToRedShiftDB(files[i], s3File.getName());
					
					writeJobLog(insertID[i], "REDSHIFTLOADEND",
							new SimpleDateFormat("HH:mm:ss").format(Calendar
									.getInstance().getTime()));

					

				} catch (AmazonServiceException ase) {

					writeLog(
							"Caught an AmazonServiceException, which means your request made it "
									+ "to Amazon S3, but was rejected with an error response for some reason."
									+ " Error Message:" + ase.getMessage()
									+ " HTTP Status Code: "
									+ ase.getStatusCode() + " AWS Error Code: "
									+ ase.getErrorCode() + " Error Type: "
									+ ase.getErrorType() + " Request Id: "
									+ ase.getRequestId(), "error", "",
							"S3Transfer", "DB");
					System.out
							.println("Caught an AmazonServiceException, which means your request made it "
									+ "to Amazon S3, but was rejected with an error response for some reason.");
					System.out.println("Error Message:    " + ase.getMessage());
					System.out.println("HTTP Status Code: "
							+ ase.getStatusCode());
					System.out.println("AWS Error Code:   "
							+ ase.getErrorCode());
					System.out.println("Error Type:       "
							+ ase.getErrorType());
					System.out.println("Request ID:       "
							+ ase.getRequestId());

					loadEndTime.add(i, "Error");

					writeJobLog(insertID[i], "Error","");

					checkList.put(files[i], "Loading Error");
					

				} catch (AmazonClientException ace) {
					writeLog(
							"Caught an AmazonClientException, which means the client encountered "
									+ "a serious internal problem while trying to communicate with S3, "
									+ "such as not being able to access the network."
									+ ace.getMessage(), "error", files[i],
							"S3Transfer", "DB");

					loadEndTime.add(i, "Error");
					

					checkList.put(files[i], "Loading Error");

					writeJobLog(insertID[i], "Error","");

				} catch (Exception e) {
					writeLog("RunID " + RunID + " " + e.getMessage(), "error",
							files[i], "S3Transfer", "DB");
					System.out.println("Error !! Please check error message "
							+ e.getMessage());

					loadEndTime.add(i, "Error");

					checkList.put(files[i], "Loading Error");

					writeJobLog(insertID[i], "Error","");
					
				} finally {
					continue;
				}

			} else {
				checkList.put(files[i], "Loading Error");
				writeLog("As there is an issue while creating the extract of "
						+ files[i] + " so loading operation is skipped for "
						+ files[i], "info", files[i], "S3Transfer", "DB");
				
				writeJobLog(insertID[i], "Error","");
				
				System.out
						.println("There is an issue while creating the extract of "
								+ files[i]
								+ ". So loading operation is skipped for "
								+ files[i]);
			}
		}

		// reviewDataLoadingSession();

	}

	public void doAmazonS3FileTransfer(char mode, String[] files) throws SecurityException, IOException {

		AWSCredentials credentials = null;
		try {
			credentials = new ProfileCredentialsProvider(awsProfile)
					.getCredentials();
		} catch (Exception e) {
			writeLog(
					"Cannot load the credentials from the credential profiles file. "
							+ "Please make sure that your credentials file is at the correct "
							+ "location, and is in valid format.", "error", "",
					"S3Transfer_Reprocess", "DB");
			throw new AmazonClientException(
					"Cannot load the credentials from the credential profiles file. "
							+ "Please make sure that your credentials file is at the correct "
							+ "location, and is in valid format.", e);

		}

		AmazonS3 s3 = new AmazonS3Client(credentials);
		Region usWest2 = Region.getRegion(Regions.US_EAST_1);
		s3.setRegion(usWest2);
		tx = new TransferManager(s3);

		SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");
		Date curDate = new Date();
		String strDate = sdf.format(curDate);

		String bucketName = extractLocationS3;
		String key = credentials.getAWSAccessKeyId();

		System.out.println("\nUploading DB Extracts to Amazon S3");
		System.out
				.println("--------------------------------------------------------");

		for (int i = 0; i < files.length; i++) {

			if (!checkErrorStatus("Extraction", files[i])) {

				writeJobLog(insertID[i], "S3LoadStart",
						new SimpleDateFormat("HH:mm:ss").format(Calendar
								.getInstance().getTime()));

				try {

					File s3File = getLastFileModifiedFile(extractLocationLocal,
							files[i]);
					writeLog("Uploading " + s3File.getName()
							+ " to Amazon S3 bucket " + bucketName, "info",
							files[i], "S3Transfer_Reprocess", "DB");
					System.out.println("Uploading " + s3File.getName()
							+ " to S3.\n");

					PutObjectRequest request = new PutObjectRequest(bucketName,
							strDate + "/" + s3File.getName(), s3File);
					upload = tx.upload(request);
					upload.waitForCompletion();
					tx.shutdownNow();
					writeJobLog(insertID[i], "S3LoadEnd",
							new SimpleDateFormat("HH:mm:ss").format(Calendar
									.getInstance().getTime()));
					
					writeLog(s3File.getName() + " transffered successfully.",
							"info", files[i], "S3Transfer_Reprocess", "DB");
					System.out.println(files[i] + " file transffered.");

					writeJobLog(insertID[i], "RedShiftLoadStart",
							new SimpleDateFormat("HH:mm:ss").format(Calendar
									.getInstance().getTime()));
					
					loadDataToRedShiftDB(files[i], s3File.getName());
					
					writeJobLog(insertID[i], "RedShiftLoadEnd",
							new SimpleDateFormat("HH:mm:ss").format(Calendar
									.getInstance().getTime()));

				} catch (AmazonServiceException ase) {

					writeLog(
							"Caught an AmazonServiceException, which means your request made it "
									+ "to Amazon S3, but was rejected with an error response for some reason.\nError Message:"
									+ ase.getMessage() + "\n"
									+ "HTTP Status Code: "
									+ ase.getStatusCode() + "\n"
									+ "AWS Error Code: " + ase.getErrorCode()
									+ "\n" + "Error Type: "
									+ ase.getErrorType() + "\n"
									+ "Request Id: " + ase.getRequestId(),
							"error", "", "S3Transfer_Reprocess", "DB");
					System.out
							.println("Caught an AmazonServiceException, which means your request made it "
									+ "to Amazon S3, but was rejected with an error response for some reason.");
					System.out.println("Error Message:    " + ase.getMessage());
					System.out.println("HTTP Status Code: "
							+ ase.getStatusCode());
					System.out.println("AWS Error Code:   "
							+ ase.getErrorCode());
					System.out.println("Error Type:       "
							+ ase.getErrorType());
					System.out.println("Request ID:       "
							+ ase.getRequestId());

					writeJobLog(insertID[i], "Error","");

				} catch (AmazonClientException ace) {
					writeLog(
							"Caught an AmazonClientException, which means the client encountered "
									+ "a serious internal problem while trying to communicate with S3, "
									+ "such as not being able to access the network."
									+ ace.getMessage(), "error", "",
							"S3Transfer", "DB");
					
					writeJobLog(insertID[i], "Error","");

					System.out.println("Error !! Please check error message "
							+ ace.getMessage());

					
				} catch (Exception e) {
					writeLog("RunID " + RunID + " " + e.getMessage(), "error",
							"", "S3Transfer_Reprocess", "DB");
					
					writeJobLog(insertID[i], "Error","");
					
					System.out.println("Error !! Please check error message "
							+ e.getMessage());

					
				} finally {
					continue;
				}

			} else {
				checkList.put(files[i], "Loading Error");
				writeLog("As there is an issue while creating the extract of "
						+ files[i] + " so loading operation is skipped for "
						+ files[i], "info", files[i], "S3Transfer_Reprocess",
						"DB");
				
				writeJobLog(insertID[i], "Error","");
				
				System.out
						.println("As there is an issue while creating the extract of "
								+ files[i]
								+ " so loading operation is skipped for "
								+ files[i]);
			}
		}

	}

	public void loadDataToRedShiftDB(String tableName, String fileName) throws SecurityException, IOException {

		Connection conn = null;
		Statement stmt = null;
		AWSCredentials credentials = null;
		String lastModDate = "";

		try {
			credentials = new ProfileCredentialsProvider(awsProfile)
					.getCredentials();
		} catch (Exception e) {
			writeLog(
					"Cannot load the credentials from the credential profiles file. "
							+ "Please make sure that your credentials file is at the correct "
							+ "location, and is in valid format.", "error", "",
					"S3Transfer", "DB");
			throw new AmazonClientException(
					"Cannot load the credentials from the credential profiles file. "
							+ "Please make sure that your credentials file is at the correct "
							+ "location, and is in valid format.", e);

		}

		AmazonS3 s3 = new AmazonS3Client(credentials);
		Region usWest2 = Region.getRegion(Regions.US_EAST_1);
		s3.setRegion(usWest2);

		String accKey = credentials.getAWSAccessKeyId();
		String scrtKey = credentials.getAWSSecretKey();

		System.out.println("RedShift Data Loading Started..");
		System.out.println("RedShift Data loading started for " + tableName);
		writeLog("RedShift Data loading started for " + tableName, "info",
				tableName, "LoadRedshift", "DB");
		try {

			// System.out.println("Waiting 1 min. for the availability of the file in Amazon S3");
			// Thread.sleep(60000);
			Class.forName("com.amazon.redshift.jdbc41.Driver");

			// Open a connection and define properties.
			System.out.println("Connecting to Redshift Cluster...");

			loadStartTimeRS.add((loadStartTimeRS.size() + 1) - 1,
					new SimpleDateFormat("HH:mm:ss").format(Calendar
							.getInstance().getTime()));

			Properties props = new Properties();

			props.setProperty("user", redShiftMasterUsername);
			props.setProperty("password", redShiftMasterUserPassword);
			conn = DriverManager.getConnection(dbURL, props);

			SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");
			Date curDate = new Date();
			String strDate = sdf.format(curDate);

			stmt = conn.createStatement();
			String sql;
			sql = "truncate table " + redShiftPreStageSchemaName + "."
					+ tableName;
			stmt.executeUpdate(sql);
			sql = "copy "
					+ redShiftPreStageSchemaName
					+ "."
					+ tableName
					+ " from 's3://"
					+ extractLocationS3
					+ "/"
					+ strDate
					+ "/"
					+ fileName
					+ "' credentials 'aws_access_key_id="
					+ accKey
					+ ";aws_secret_access_key="
					+ scrtKey
					+ "' timeformat 'YYYY-MM-DD HH:MI:SS' escape removequotes delimiter as ',' IGNOREHEADER 1 ACCEPTINVCHARS;";
			System.out.println("Executing Query..");
			writeLog("Executing Query..\n" + sql, "info", tableName,
					"LoadRedshift", "DB");

			stmt.executeUpdate(sql);

			System.out.println("Done..");

			loadEndTimeRS.add((loadEndTimeRS.size() + 1) - 1,
					new SimpleDateFormat("HH:mm:ss").format(Calendar
							.getInstance().getTime()));

			checkList.put(tableName, "Loading Done");
			writeLog("RedShift Data loading is completed successfully for "
					+ tableName, "info", tableName, "LoadRedshift", "DB");
			stmt.close();
			conn.close();

			for (String s : this.facts) {
				if (s.equals(tableName)) {

					Properties p = new Properties();
					File pf = new File("tmpFile.properties");
					p.load(new FileReader(pf));
					lastModDate = p.getProperty(tableName);
					updateFactsProperty(factsPropFile, tableName, lastModDate);
				}
			}

		} catch (Exception ex) {
			loadEndTimeRS.add((loadEndTimeRS.size() + 1) - 1, "error");
			checkList.put(tableName, "Loading Error");
			System.out
					.println("Error occured while loading data from S3 to Redshift Cluster for "
							+ tableName);

			writeLog(
					"RunID "
							+ RunID
							+ " Error occured while loading data from S3 to Redshift Cluster for "
							+ tableName + " " + ex.getMessage(), "error",
					tableName, "LoadRedshift", "DB");

		} finally {
			// Finally block to close resources.
			try {
				if (stmt != null)
					stmt.close();
			} catch (Exception ex) {
			}// nothing we can do
			try {
				if (conn != null)
					conn.close();
			} catch (Exception ex) {
				ex.printStackTrace();
			}
		}

	}

	public void createDbExtractFromURL() throws IOException {

		ICsvMapReader mapReader = null;
		ICsvMapWriter mapWriter = null;

		SimpleDateFormat sdf = new SimpleDateFormat("ddMMyyyyhhmmss");
		Date curDate = new Date();
		String strDate = sdf.format(curDate);

		try {
			System.out.println("Fetching Flat file from URL " + ExtractURL
					+ " and storing the file in " + extractLocationURL
					+ " location");

			writeLog("Fetching Flat file from URL " + ExtractURL
					+ " and storing the file in " + extractLocationURL
					+ " location", "info", "CustomForm", "URLDataExtraction",
					"DB");
			
			
			insertID[insertID.length] = writeJobLog(getRunID(), "CustomForm",
					"Normal", "In-Progress");
			
			writeJobLog(insertID[insertID.length], "EXTRACTSTART",
					new SimpleDateFormat("HH:mm:ss").format(Calendar
							.getInstance().getTime()));
			
			
			URL website = new URL(ExtractURL);
			ReadableByteChannel rbc = Channels.newChannel(website.openStream());
			FileOutputStream fos = new FileOutputStream(this.extractLocationURL
					+ File.separator + "custom_form_tmp_" + strDate + ".csv");
			fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);

			CsvPreference prefs = CsvPreference.STANDARD_PREFERENCE;
			mapReader = new CsvMapReader(new FileReader(this.extractLocationURL
					+ File.separator + "custom_form_tmp_" + strDate + ".csv"),
					prefs);
			mapWriter = new CsvMapWriter(new FileWriter(this.extractLocationURL
					+ File.separator + "custom_form_" + strDate + ".csv"),
					prefs);

			// header used to read the original file
			final String[] readHeader = mapReader.getHeader(true);

			// header used to write the new file
			// (same as 'readHeader', but with additional column)
			final String[] writeHeader = new String[readHeader.length + 1];
			System.arraycopy(readHeader, 0, writeHeader, 0, readHeader.length);
			final String timeHeader = "RunID";
			writeHeader[writeHeader.length - 1] = writeHeader[0];
			writeHeader[0] = timeHeader;

			mapWriter.writeHeader(writeHeader);

			Map<String, String> row;
			while ((row = mapReader.read(readHeader)) != null) {

				// add your column with desired value
				row.put(timeHeader, String.valueOf(this.getRunID()));

				mapWriter.write(row, writeHeader);
			}
			
			writeJobLog(insertID[insertID.length], "EXTRACTEND",
					new SimpleDateFormat("HH:mm:ss").format(Calendar
							.getInstance().getTime()));

		} catch (IOException e) {
			System.out.println("Error !! Please check log for details");
			
			writeLog("Error \n " + e.getMessage(), "error", "CustomForm",
					"URLDataExtraction", "DB");
			
			writeJobLog(insertID[insertID.length], "Error","");

		} finally {
			if (mapReader != null) {
				try {
					mapReader.close();
				} catch (IOException e) {
					System.out.println("Error !! Please check log for details");
					writeLog("Error \n " + e.getMessage(), "error",
							"CustomForm", "URLDataExtraction", "DB");
				}
			}
			if (mapWriter != null) {
				try {
					mapWriter.close();
				} catch (IOException e) {
					System.out.println("Error !! Please check log for details");
					writeLog("Error \n " + e.getMessage(), "error",
							"CustomForm", "URLDataExtraction", "DB");
				}
			}
		}

	}

	public void reviewDataLoadingSession() throws SecurityException, IOException {
		int errCount = 0;
		int successCount = 0;
		System.out.println("DataLoading Operation completed.");

		for (Map.Entry<String, String> entry : checkList.entrySet()) {
			if (entry.getValue().equalsIgnoreCase("Extraction Error")) {

				errCount++;

			} else if (entry.getValue().equalsIgnoreCase("Loading Error")) {

				errCount++;

			} else {
				successCount++;
			}
		}

		if (errCount > 0) {
			writeLog(
					"RunID "
							+ RunID
							+ " Out of "
							+ (dimensions.length + facts.length)
							+ "tables "
							+ successCount
							+ " tables are loaded successfully. \n Program will try to load all the failure tables in the next run",
					"info", "", "FinalReview", "DB");
			System.out
					.println("Out of "
							+ (dimensions.length + facts.length)
							+ "tables "
							+ successCount
							+ " tables are loaded successfully. \n Program will try to load all the failure tables in the next run");
			cleanResources();
			System.exit(0);

		} else {
			writeLog("RunID " + RunID + " All tables are loaded sucessfully",
					"info", "", "FinalReview", "DB");
			System.out.println("All tables are loaded successfully");
			cleanResources();
			System.exit(0);
		}
	}

	// Utility methods
	public int convertToNumber(String value, String propertyName) {
		try {
			return Integer.valueOf(value);
		} catch (NumberFormatException e) {
			throw new RuntimeException(propertyName + " must be a number: "
					+ value);
		}
	}

	public File getLastFileModifiedFile(String dir, String prefix) {
		File fl = new File(dir);
		File[] files = fl.listFiles(new FileFilter() {
			public boolean accept(File file) {
				return file.isFile();
			}
		});
		long lastMod = Long.MIN_VALUE;
		File choice = null;
		for (File file : files) {
			if ((file.lastModified() > lastMod)
					&& (file.getName().startsWith(prefix))) {
				choice = file;
				lastMod = file.lastModified();
			}
		}
		return choice;
	}

	private List<String> getColumnNames(ResultSet resultSet)
			throws SQLException {
		List<String> columnNames = new ArrayList<String>();
		ResultSetMetaData metaData = resultSet.getMetaData();
		columnNames.add("RunID");
		for (int i = 0; i < metaData.getColumnCount(); i++) {
			// indexing starts from 1
			columnNames.add(metaData.getColumnName(i + 1));
		}
		return columnNames;
	}

	private void cleanResources() {
		if (resultSet != null) {
			try {
				resultSet.close();
			} catch (SQLException e) {
			}
		}
		if (statement != null) {
			try {
				statement.close();
			} catch (SQLException e) {
			}
		}
		if (con != null) {
			try {
				con.close();
			} catch (SQLException e) {
			}
		}
	}

	private void writeLog(String msg, String type, String entity, String stage, String appender) throws /*SecurityException, */ IOException{

		String logSql = "";
		PreparedStatement ps;

	 	Logger logger = Logger.getLogger("AppLog");
		logger.setUseParentHandlers(false);

		if(appender.equals("file")) {
			
			/*Logger logger = Logger.getLogger("AppLog");
			logger.setUseParentHandlers(false);*/
			
			switch (type) {
			case "info":
				try {
					
					FileHandler fh = new FileHandler(logLocation + File.separator
							+ "App.log", true);
					logger.addHandler(fh);
					SimpleFormatter formatter = new SimpleFormatter();
					fh.setFormatter(formatter);
					logger.info(msg);
					fh.close();
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
				Connection connection = DriverManager.getConnection(logDbURL,logDbUid,logDbPwd);
				Calendar calendar = Calendar.getInstance();
		    	Timestamp currentTimestamp = new java.sql.Timestamp(calendar.getTime().getTime());
		    	
		    	logSql = "INSERT INTO message_log(runid,message_desc,target_table,message_stage,message_type,message_timestamp) "
						+ "VALUES(?,?,?,?,?,?)";
		    	ps = connection.prepareStatement(logSql);
				ps.setInt(1, this.getRunID());
		    	ps.setString(2,msg);
				ps.setString(3,entity);
				ps.setString(4,stage);
				ps.setString(5,type);
				ps.setTimestamp(6, currentTimestamp);
				ps.executeUpdate();
				//connection.commit();
				connection.close();
				
			} catch (SQLException e1) { //Modified to capture if MySql is down
				//e1.printStackTrace();
				FileHandler fh = new FileHandler(logLocation + File.separator+ "App.log", true); // can throw IOException
				logger.addHandler(fh);
				SimpleFormatter formatter = new SimpleFormatter();
				fh.setFormatter(formatter);
				logger.severe("Error in writing message_log!! "+e1.getMessage());
				fh.close();
				System.exit(0);
			}
		}

	}

	private void writeStatusReport(int RunID, String tblName, String extStart,
			String extEnd, String loadStart, String loadEnd,
			String loadStartRS, String loadEndRS, boolean headers)
			throws ParseException {

		String Status, content;
		if (extEnd.equalsIgnoreCase("error")) {
			Status = "Error during Data Extraction";
			content = RunID + "\t\t\t" + tblName + "\t\t\t" + extStart
					+ "\t\t--\t\t" + loadStart + "\t\t" + loadEnd + "\t\t"
					+ Status;
		} else if (loadEnd.equalsIgnoreCase("error")) {
			Status = "Datafile created but S3 load error";
			content = RunID + "\t\t\t" + tblName + "\t\t\t" + extStart + "\t\t"
					+ extEnd + "\t\t" + loadStart + "\t\t--\t\t" + Status;
		} else if (loadEndRS.equalsIgnoreCase("error")) {

			Status = "Datafile created and loaded in S3 but Redshift load error";
			content = RunID + "\t\t\t" + tblName + "\t\t\t" + extStart + "\t\t"
					+ extEnd + "\t\t" + loadStart + "\t\t" + loadEnd + "\t\t"
					+ loadStartRS + "\t\t--\t\t" + Status;

		} else {
			Status = "Loaded";
			content = RunID + "\t\t\t" + tblName + "\t\t\t" + extStart + "\t\t"
					+ extEnd + "\t\t" + loadStart + "\t\t" + loadEnd + "\t\t"
					+ loadStartRS + "\t\t" + loadEndRS + "\t\t" + Status;
		}

		String FileName = statusRptLocation + File.separator
				+ "Status_Report.txt";

		File file = new File(FileName);

		try (PrintWriter out = new PrintWriter(new BufferedWriter(
				new FileWriter(file, true)))) {

			if (headers) {
				out.println("RunID \t\t\tTable\t\t\tExtractStartTime\t\tExtractEndTime\t\tLoadStartTime(S3)\t\tLoadEndTime(S3)\t\tLoadStartTime(Redshift)\t\tLoadEndTime(Redshift)\t\tStatus");
			}

			out.println(content);
			out.flush();
			out.close();
		}

		catch (IOException e) {

			e.printStackTrace();
		}

	}

	public String[] getListOfDimensionsFacts() {
		String[] files = null;
		if (dimensions != null && facts != null) {
			files = combine(dimensions, facts);
		} else if (dimensions != null && facts == null) {
			files = dimensions;
		} else {
			files = facts;

		}
		return files;
	}

	private void updateFactsProperty(String propName, String key, String value)
			throws FileNotFoundException {

		Properties props = new Properties();
		Writer writer = null;
		File f = new File(propName);
		if (f.exists()) {
			try {

				props.load(new FileReader(f));
				props.setProperty(key.trim(), value.trim());

				writer = new FileWriter(f);
				props.store(writer, null);
				writer.close();
			} catch (IOException e) {

				e.printStackTrace();
			}

		} else {
			throw new FileNotFoundException("Invalid Properties file or "
					+ propName + " not found");
		}
	}

	private String[] combine(String[] a, String[] b) {
		int length;
		String[] result = null;
		if (a != null && b != null) {

			length = a.length + b.length;
			result = new String[length];
			System.arraycopy(a, 0, result, 0, a.length);
			System.arraycopy(b, 0, result, a.length, b.length);

		} else if (!a.equals(null)) {

			result = a;

		} else {

			result = b;
		}

		return result;
	}
	
	public long writeJobLog(int runID, String entity, String run_mode,
			String job_status) {

		String logSql = "";
		long key = -1L;

		try {
			Connection connection = DriverManager.getConnection(logDbURL,
					logDbUid, logDbPwd);

			logSql = "INSERT INTO job_log(runid,entity,run_mode,job_status) "
					+ "VALUES(?,?,?,?)";

			PreparedStatement ps = connection.prepareStatement(logSql,
					Statement.RETURN_GENERATED_KEYS);
			ps.setInt(1, runID);
			ps.setString(2, entity);
			ps.setString(3, run_mode);
			ps.setString(4, job_status);
			ps.executeUpdate();
			ResultSet rs = statement.getGeneratedKeys();
			if (rs != null && rs.next()) {
				key = rs.getLong(1);
			}

		} catch (Exception e) {

			System.exit(0);
		}
		return key;
	}

	public void writeJobLog(long key, String optName, String optTime) {

		Connection connection;
		String logSql = "";
		try {
			connection = DriverManager.getConnection(logDbURL, logDbUid,
					logDbPwd);
			switch (optName.toUpperCase()) {

			case "EXTRACTSTART":

				logSql = "UPDATE job_log SET ExtractStart = ? WHERE job_id = ?) ";
				break;

			case "EXTRACTEND":

				logSql = "UPDATE job_log SET ExtractEnd = ? WHERE job_id = ?) ";
				break;

			case "S3LOADSTART":

				logSql = "UPDATE job_log SET S3LoadStart = ? WHERE job_id = ?) ";
				break;

			case "S3LOADEND":

				logSql = "UPDATE job_log SET S3LoadEnd = ? WHERE job_id = ?) ";
				break;

			case "REDSHIFTLOADSTART":

				logSql = "UPDATE job_log SET RedShiftLoadStart = ? WHERE job_id = ?) ";
				break;

			case "REDSHIFTLOADEND":

				logSql = "UPDATE job_log SET RedShiftLoadEnd = ? WHERE job_id = ?) ";
				break;

			case "ERROR":
				logSql = "UPDATE job_log SET job_status = ? WHERE job_id = ?) ";
				break;
				
			default:

				logSql = "UPDATE job_log SET job_status = ? WHERE job_id = ?) ";
				break;

			}

			if (!optName.equalsIgnoreCase("error")) {
				PreparedStatement ps = connection.prepareStatement(logSql);
				Timestamp ts = Timestamp.valueOf(optTime);
				ps.setTimestamp(1, ts);
				ps.setInt(2, (int) key);
				ps.executeUpdate();
				
			} else {

				PreparedStatement ps = connection.prepareStatement(logSql);

				ps.setString(1, "Error");
				ps.setInt(2, (int) key);
				ps.executeUpdate();
			}

		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.exit(0);
		}

		// Calendar calendar = Calendar.getInstance();
		// Timestamp currentTimestamp = new
		// java.sql.Timestamp(calendar.getTime().getTime());

	}

	private boolean checkErrorStatus(String errorType, String tabName) {

		String status = checkList.get(tabName);
		boolean errorStatus = false;

		switch (errorType) {
		case "Extraction":
			if (status.equalsIgnoreCase("Extraction Error"))
				errorStatus = true;
			break;

		case "Loading":
			if (status.equalsIgnoreCase("Loading Error"))
				errorStatus = true;
			break;
		}
		return errorStatus;

	}

	public Connection createRedShiftDbCon() {

		Connection con = null;

		try {
			Class.forName("com.amazon.redshift.jdbc41.Driver");
			Properties props = new Properties();
			props.setProperty("user", redShiftMasterUsername);
			props.setProperty("password", redShiftMasterUserPassword);
			con = DriverManager.getConnection(dbURL, props);
		} catch (ClassNotFoundException e) {
			System.out.println("Error::createRedShiftDbCon()->"
					+ e.getMessage());
		} catch (SQLException e) {
			System.out.println("Error::createRedShiftDbCon()->"
					+ e.getMessage());
		}

		return con;

	}

	public void listFilesForFolder(final File folder) {
		for (final File fileEntry : folder.listFiles()) {
			if (fileEntry.isDirectory()) {
				listFilesForFolder(fileEntry);
			} else {
				System.out.println(fileEntry.getName());
			}
		}
	}

	public void updateRunID(int runID) {

		try {
			updateFactsProperty(appConfigPropFile, "RunID ",
					String.valueOf(runID));
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

}
