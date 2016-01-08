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


//import org.apache.log4j.Logger;
//import org.apache.log4j.PropertyConfigurator;
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
	private String redShiftMasterUsername;
	private String redShiftMasterUserPassword;
	private String redShiftPreStageSchemaName;
	private String dbURL;
	private Map<String, String> checkList = new HashMap<String, String>();
	private char csvDelimiter;
	private int RunID;
//	private String eol;
//	private String aSQLScriptFilePath;
	private File tmpLog;
	private String ExtractURL;
	private String logDbURL;
	private String logDbUid;
	private String logDbPwd;
	private long[] insertID;
	private List<Long> insertIDList;
	private final String[] DIM;

	public DataLoader() throws IOException, /*ClassNotFoundException,*/
			SQLException {
		
		/*Logger logger = Logger.getLogger(DataLoader.class);
		PropertyConfigurator.configure("log4j.properties");*/
		appConfigPropFile = "config.properties";
		Properties properties = new Properties();
		File pf = new File(appConfigPropFile);
		properties.load(new FileReader(pf));

		factsPropFile = "facts.properties";
//		eol = System.getProperty("line.separator");

		serverHost = properties.getProperty("ServerHost");
		serverDataSource = properties.getProperty("DataSource");
		login = properties.getProperty("Login");
		password = properties.getProperty("Password");
		accountId = properties.getProperty("AccountId");
		roleId = convertToNumber(properties.getProperty("RoleId"), "RoleID");
		port = convertToNumber(properties.getProperty("Port"), "Port");

		logDbURL = properties.getProperty("LogDBURL");
		logDbUid = properties.getProperty("LogDBUID");
		logDbPwd = properties.getProperty("LogDBPwd");
		
		RunID = Integer.parseInt(properties.getProperty("RunID"));
		RunID++;
		
		if (!properties.getProperty("Dimensions").isEmpty() && !properties.getProperty("Dimensions").equalsIgnoreCase("NONE")) {
			dimensions = properties.getProperty("Dimensions").split(",");
		} else {
			System.out.println("No dimentions are mentioned in the config file. Facts processing cannot be done without dimension, hence terminating the program.");
			writeLog("Error !! No dimentions are mentioned in the config file. Facts processing cannot be done without dimension, hence terminating the program.",
					"error","","Aplication Startup","db");
			System.exit(0);
		}

		if (!properties.getProperty("Facts").isEmpty() && !properties.getProperty("Facts").equalsIgnoreCase("NONE")) {
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

		factsSqlScriptLocation = properties.getProperty("FactFileLoc");

		awsProfile = properties.getProperty("AwsProfile");
		redShiftMasterUsername = properties.getProperty("RSUID");
		redShiftMasterUserPassword = properties.getProperty("RSPWD");
		redShiftPreStageSchemaName = properties.getProperty("RSSCHEMAPRESTAGE");
		dbURL = properties.getProperty("RSDBURL");
		csvDelimiter = properties.getProperty("CSVDelim").charAt(0);
		ExtractURL = properties.getProperty("FileExtractURL");

		updateFactsProperty(appConfigPropFile, "RunID ", String.valueOf(RunID));

		DIM = properties.getProperty("Dimensions1").split(",");

		try {
			new File(extractLocationLocal).mkdirs();
			new File(extractLocationURL).mkdirs();
			new File(logLocation).mkdirs();

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

		}/* catch (SecurityException e) {
			System.out
					.println("Application Can't create Log Directory. See Error Message for mor details."
							+ e.getMessage());

			tmpLog = new File("tmp.log");
			FileWriter fWriter = new FileWriter(tmpLog);
			PrintWriter pWriter = new PrintWriter(fWriter);
			pWriter.println("Application Can't create Log Directory. See Error Message for mor details."
					+ e.getMessage());

			System.exit(0);

		}*/ catch (ClassNotFoundException e) {
			System.out.println("Error !! Please check error message "
					+ e.getMessage());
			writeLog("RunID " + RunID + "Error !! Please check error message. "
					+ e.getMessage(), "error", "", "Aplication Startup", "db");
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
//		eol = System.getProperty("line.separator");

		// Initialize the class variables with properties values
		serverHost = properties.getProperty("ServerHost");
		serverDataSource = properties.getProperty("DataSource");
		login = properties.getProperty("Login");
		password = properties.getProperty("Password");
		accountId = properties.getProperty("AccountId");
		roleId = convertToNumber(properties.getProperty("RoleId"), "RoleID");
		port = convertToNumber(properties.getProperty("Port"), "Port");
		
		RunID = runid;
		
		logDbURL = properties.getProperty("LogDBURL");
		logDbUid = properties.getProperty("LogDBUID");
		logDbPwd = properties.getProperty("LogDBPwd");
		
		if (!properties.getProperty("Dimensions").equalsIgnoreCase("NONE")) {
			dimensions = properties.getProperty("Dimensions").split(",");
		} else {
			//dimensions = null;
			System.out.println("No dimentions are mentioned in the config file. Facts processing cannot be done without dimension, hence terminating the program.");
			writeLog("Error !! No dimentions are mentioned in the config file. Facts processing cannot be done without dimension, hence terminating the program.",
					"error","","Aplication Startup","db");
			System.exit(0);
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

//		aSQLScriptFilePath = properties.getProperty("SQLScriptsPath");
		awsProfile = properties.getProperty("AwsProfile");
		redShiftMasterUsername = properties.getProperty("RSUID");
		redShiftMasterUserPassword = properties.getProperty("RSPWD");
		redShiftPreStageSchemaName = properties.getProperty("RSSCHEMAPRESTAGE");
		dbURL = properties.getProperty("RSDBURL");
		csvDelimiter = properties.getProperty("CSVDelim").charAt(0);
		
		DIM = properties.getProperty("Dimensions1").split(",");
		
		
		try {
			new File(extractLocationLocal).mkdirs();
			new File(logLocation).mkdirs();

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
		String[] dimensionsArr = dimensions;
		String[] factsArr = facts;
		createDBExtract(dimensionsArr, factsArr, CommonConstants.RUNMODE_NORMAL, CommonConstants.DATAEXTRACTION);

	}

	public void createDBExtract(String[] dimArr, String[] factArr, String runMode,String process) throws IOException, FileNotFoundException {
		insertIDList = new ArrayList<>();
		String TBL_DEF_CONF = "tbl_def.properties";
		String factFileName;

		Properties properties = new Properties();
		File pf = new File(TBL_DEF_CONF);
		properties.load(new FileReader(pf));

		for (int i = 0; i < dimArr.length; i++) {

			System.out
			.println("DataExtraction Operation Started for DB table "
					+ dimArr[i]);
			System.out.println("--------------------------------------------------------");

			writeLog("RunID " + RunID
					+ " DataExtraction Operation Started for DB table "
					+ dimArr[i], "info", dimArr[i],
					process, "db");

			insertIDList.add(writeJobLog(getRunID(), dimArr[i], runMode, "In-Progress"));

			// LOAD USER SPECIFIC COLUMNS FROM THE TABLE DEFINED IN PROPERTIES FILE

			query = properties.getProperty(dimArr[i]);

			try {

				writeJobLog(insertIDList.get(insertIDList.size()-1), "EXTRACTSTART",
						new SimpleDateFormat("YYYY-MM-DD HH:mm:ss").format(Calendar
								.getInstance().getTime()));

				writeLog("RunID " + RunID + " Retrieving data for "
						+ dimArr[i], "info", dimArr[i],
						process, "db");

				System.out.println("Retrieving data...");

				statement = con.createStatement();

				System.out.println("Executing query for " + dimArr[i]
						+ " table.");

				resultSet = statement.executeQuery(query);

				SimpleDateFormat sdf = new SimpleDateFormat(
						"ddMMyyyyHHmmss"); 
				Date curDate = new Date();
				String strDate = sdf.format(curDate);

				String fileName = extractLocationLocal + File.separator
						+ dimArr[i] + "-" + "RunID-" + RunID + "-"
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

				writeJobLog(insertIDList.get(insertIDList.size()-1), "EXTRACTEND",
						new SimpleDateFormat("YYYY-MM-DD HH:mm:ss").format(Calendar
								.getInstance().getTime()));

				File csvFile = new File(fileName);

				writeLog("RunID " + RunID
						+ " DataExtraction Operation for table "
						+ dimArr[i] + " is extracted in " + fileName,
						"info", dimArr[i], process, "db");

				System.out.println("DataExtraction Operation for table "
						+ dimArr[i] + " is extracted in " + fileName);

				checkList.put(dimArr[i], "Extraction Done");

			} catch (SQLException e) {
				System.out
				.println("SQL Error!! Please Check the query and try again.\n"
						+ e.getMessage());

				checkList.put(dimArr[i], "Extraction Error");

				writeLog(
						"RunID " + RunID + " SQL Error!!" + e.getMessage(),
						"error", dimArr[i], process, "db");

				writeJobLog(insertIDList.get(insertIDList.size()-1), "Error", "");

			} catch (IOException e) {

				System.out
				.println("IO Error!! Please check the error message.\n"
						+ e.getMessage());

				checkList.put(dimArr[i], "Extraction Error");

				writeLog("RunID " + RunID + " Error!!" + e.getMessage(),
						"error", dimArr[i], process, "db");

				writeJobLog(insertIDList.get(insertIDList.size()-1), "Error", "");

			} catch (Exception e) {
				System.out.println("RunID " + RunID
						+ " Error!! Please check the error message.\n"
						+ e.getMessage());

				checkList.put(dimArr[i], "Extraction Error");

				writeLog("RunID " + RunID + " Error!!" + e.getMessage(),
						"error", dimArr[i], process, "db");

				writeJobLog(insertIDList.get(insertIDList.size()-1), "Error", "");

			} finally {
				continue;
			}

		}

		/***************************** Fact Extraction Started **************************************/

		if (factArr != null) {
			int idex = dimArr.length - 1;
			System.out.println("DataExtraction Operation Started for facts.");

			writeLog("RunID " + RunID
					+ " DataExtraction Operation Started for facts.", "info",
					"", process, "db");
			for (int x = 0; x < factArr.length; x++) {
				insertIDList.add(writeJobLog(getRunID(), factArr[x],runMode, "In-Progress"));

				writeJobLog(insertIDList.get(insertIDList.size()-1), "EXTRACTSTART",
						new SimpleDateFormat("YYYY-MM-DD HH:mm:ss").format(Calendar
								.getInstance().getTime()));

				idex = idex + x + 1;

				try {

					factFileName = factsSqlScriptLocation + File.separator
							+ factArr[x] + ".sql";

					createFactExtract(con, factFileName, factArr[x], idex, process);

					writeJobLog(insertIDList.get(insertIDList.size()-1), "EXTRACTEND",
							new SimpleDateFormat("YYYY-MM-DD HH:mm:ss").format(Calendar
									.getInstance().getTime()));

				} catch (Exception e) {

					checkList.put(factArr[x], "Extraction Error");

					writeLog("RunID " + RunID + " Error!!" + e.getMessage(),
							"error", factArr[x], process, "db");

					writeJobLog(insertIDList.get(insertIDList.size()-1), "Error","");


				} finally {
					continue;
				}
			}
		} else {
			System.out.println("No facts are mentioned in the config file.");
		}
	}

	public void createDbExtract(char mode, String[] failedFileList)	throws IOException {
		List<String> dimList = new ArrayList<>();
		List<String> factList = new ArrayList<>();
		for(String file: failedFileList) {
			if(Arrays.asList(DIM).contains(file)) {
				dimList.add(file);
			} else {
				factList.add(file);
			}
		}
		
		String[] dimArr = dimList.toArray( new String[dimList.size()]);
		String[] factArr = factList.toArray( new String[factList.size()]);
		createDBExtract(dimArr, factArr, CommonConstants.RUNMODE_REPROCESS, CommonConstants.DATAEXTRACTION_REPROCESS);

	}

	private void createFactExtract(Connection conn, String factFile,
			String factName, int pos, String process) throws SQLException, IOException {

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
					lastModDate = tmpdt[1];
					line = String.format(line, lastModDate, lastModDate);
					writeLog("RunID " + RunID + " Executing query for "
							+ factName + "where DATE_LAST_MODIFIED >= "
							+ lastModDate, "info", factName, process,
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
								factName, process, "DB");

						SimpleDateFormat sdf = new SimpleDateFormat(
								"ddMMyyyyHHmmss");
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
							row.add(RunID);
							for (int k = 0; k <columnNames.size() - 1; k++) {
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
							/*extractEndTime.add(pos, new SimpleDateFormat(
									"HH:mm:ss").format(Calendar.getInstance()
									.getTime()));*/
						}

						System.out.println("Extraction Completed for fact "
								+ factName);
						writeLog("RunID " + RunID
								+ " Extraction Completed for fact " + factName,
								"info", factName, process, "DB");
						//TODO joblog entry is in calling method
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
								process, "DB");
						if (pos >= 0) {
							checkList.put(factName, "Extraction Error");
							//extractEndTime.add(pos, "Error");
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
					"", process, "DB");
			throw new FileNotFoundException(e.toString());

		} catch (SQLException e) {

			writeLog("RunID " + RunID + " Error!!" + e.getMessage(), "error",
					"", process, "DB");
			throw new SQLException();

		} catch (IOException e) {

			writeLog("RunID " + RunID + " Error!!" + e.getMessage(), "error",
					"", process, "DB");
			throw new IOException(e.toString());

		} 

	}

	public void doAmazonS3FileTransfer() throws SecurityException, IOException {

 		String[] files = combine(dimensions, facts);
		s3AndRedShiftUpload(files, CommonConstants.S3_TRANSFER); 

	}

	public void s3AndRedShiftUpload(String[] files, String process) throws IOException {
		AWSCredentials credentials = null;
		try {

			credentials = new ProfileCredentialsProvider(awsProfile)
					.getCredentials();
		} catch (Exception e) {
			writeLog(
					"Cannot load the credentials from the credential profiles file. "
							+ "Please make sure that your credentials file is at the correct "
							+ "location, and is in valid format.", "error", "",
					process, "DB");
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
		System.out.println("--------------------------------------------------------");

		for (int i = 0; i < files.length; i++) {
			
			if (!checkErrorStatus("Extraction", files[i])) {
								
				writeJobLog(insertIDList.get(i), "S3LOADSTART",
						new SimpleDateFormat("YYYY-MM-DD HH:mm:ss").format(Calendar
								.getInstance().getTime()));

				try {

					File s3File = getLastFileModifiedFile(extractLocationLocal,
							files[i]);
					writeLog(
							"RunID " + RunID + " Uploading " + s3File.getName()
									+ " to Amazon S3 bucket " + bucketName,
							"info", files[i], process, "DB");
					System.out.println("Uploading " + s3File.getName()
							+ " to S3.\n");

					PutObjectRequest request = new PutObjectRequest(bucketName,
							strDate + "/" + s3File.getName(), s3File);
					upload = tx.upload(request);
					upload.waitForCompletion();
					if(upload.getState().ordinal()==2)
					 {
					 
					writeJobLog(insertIDList.get(i), "S3LOADEND",
							new SimpleDateFormat("YYYY-MM-DD HH:mm:ss").format(Calendar
									.getInstance().getTime()));
					
					writeLog("RunID " + RunID +" "+ s3File.getName()
							+ " transferred successfully.", "info", files[i],
							process, "DB");
					
					System.out.println(files[i] + " file transferred.");
					
					writeJobLog(insertIDList.get(i), "REDSHIFTLOADSTART",
							new SimpleDateFormat("YYYY-MM-DD HH:mm:ss").format(Calendar
									.getInstance().getTime()));
					
					if(proceedWithFactsLoad(files[i],i))
						loadDataToRedShiftDB(files[i], s3File.getName(), i);
					
				 }

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
							process, "DB");
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

					writeJobLog(insertIDList.get(i), "Error","");

					checkList.put(files[i], "Loading Error");
					

				} catch (AmazonClientException ace) {
					writeLog(
							"Caught an AmazonClientException, which means the client encountered "
									+ "a serious internal problem while trying to communicate with S3, "
									+ "such as not being able to access the network."
									+ ace.getMessage(), "error", files[i],
							process, "DB");

					checkList.put(files[i], "Loading Error");

					writeJobLog(insertIDList.get(i), "Error","");

				} catch (Exception e) {
					writeLog("RunID " + RunID + " " + e.getMessage(), "error",
							files[i], process, "DB");
					System.out.println("Error !! Please check error message "
							+ e.getMessage());

					checkList.put(files[i], "Loading Error");

					writeJobLog(insertIDList.get(i), "Error","");
					
				} finally {
					continue;
				}

			} else {
				checkList.put(files[i], "Loading Error");   //TODO Showing this exception in log
				writeLog("As there is an issue while creating the extract of "
						+ files[i] + " so loading operation is skipped for "
						+ files[i], "info", files[i], process, "DB");
				
	//			writeJobLog(insertIDList.get(i), "Error",""); //TODO Can be removed as already in error state
				
				System.out
						.println("There is an issue while creating the extract of "
								+ files[i]
								+ ". So loading operation is skipped for "
								+ files[i]);
			}
		}

		tx.shutdownNow();
	}

	
	private boolean proceedWithFactsLoad(String fact, int listIndex) throws IOException {
		boolean proceed = true;
		ArrayList<String> factList = new ArrayList<>(Arrays.asList(facts));
		if(factList.contains(fact)) {
			for(String dimension : dimensions) {
				boolean dimLoadError = checkErrorStatus("Loading", dimension);
				if(dimLoadError) {
					proceed = false;
					System.out.println("There is an error in copying dimension "+dimension+" to Redshift, hence fact will not work properly, aborting the program.");
					writeLog(
							"There is an error in copying dimension "+dimension
							+" to Redshift, hence fact will not work properly, aborting the program.", 
							"error", fact,"LoadRedshift", "DB");

					writeJobLog(insertIDList.get(listIndex), "Error","");
					break;
				}
			}
		}
		return proceed;
	}	

	public void doAmazonS3FileTransfer(String[] files) throws SecurityException, IOException {
		s3AndRedShiftUpload(files, CommonConstants.S3_TRANSFER_REPROCESS); 
	}

	public void loadDataToRedShiftDB(String tableName, String fileName, int listIndex) throws SecurityException, IOException {

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

		/*	loadStartTimeRS.add((loadStartTimeRS.size() + 1) - 1,
					new SimpleDateFormat("HH:mm:ss").format(Calendar
							.getInstance().getTime()));*/

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

			/*loadEndTimeRS.add((loadEndTimeRS.size() + 1) - 1,
					new SimpleDateFormat("HH:mm:ss").format(Calendar
							.getInstance().getTime()));*/

			checkList.put(tableName, "Loading Done");
			writeLog("RedShift Data loading is completed successfully for "
					+ tableName, "info", tableName, "LoadRedshift", "DB");
			
			writeJobLog(insertIDList.get(listIndex), "REDSHIFTLOADEND",
					new SimpleDateFormat("YYYY-MM-DD HH:mm:ss").format(Calendar
							.getInstance().getTime()));
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
		//	loadEndTimeRS.add((loadEndTimeRS.size() + 1) - 1, "error");
			checkList.put(tableName, "Loading Error");
			System.out
					.println("Error occured while loading data from S3 to Redshift Cluster for "
							+ tableName+ " " + ex.getMessage()+" hence aborting the program.");

			writeLog(
					"RunID "
							+ RunID
							+ " Error occured while loading data from S3 to Redshift Cluster for "
							+ tableName + " " + ex.getMessage()+" hence aborting the program.", "error",
					tableName, "LoadRedshift", "DB");
			
			writeJobLog(insertIDList.get(listIndex), "ERROR",
					new SimpleDateFormat("YYYY-MM-DD HH:mm:ss").format(Calendar
							.getInstance().getTime())); 
			System.exit(0);

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

		SimpleDateFormat sdf = new SimpleDateFormat("ddMMyyyyHHmmss");
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
					new SimpleDateFormat("YYYY-MM-DD HH:mm:ss").format(Calendar
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

				
				row.put(timeHeader, String.valueOf(this.getRunID()));

				mapWriter.write(row, writeHeader);
			}
			
			writeJobLog(insertID[insertID.length], "EXTRACTEND",
					new SimpleDateFormat("YYYY-MM-DD HH:mm:ss").format(Calendar
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

	public void writeLog(String msg, String type, String entity, String stage, String appender) throws /*SecurityException, */ IOException{

		String logSql = "";
		PreparedStatement ps;

	 	Logger logger = Logger.getLogger("AppLog");
	//	logger.setUseParentHandlers(false);

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
				//	logger.error(msg);
				

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
				logger.severe("Error in writing message_log!!  Hence terminating the program. "+e1.getMessage());
				fh.close();
			//	System.out.println(" Error in writing message_log!!  Hence terminating the program. ");
			//	logger.error("Error in writing message_log!! "+e1.getMessage());
				System.exit(0);
			}
		}

	}
	
	public void updateConfig(String property,String value) {
		
		PreparedStatement ps;
		String logSql = "";
		
		try {
			Connection connection = DriverManager.getConnection(logDbURL,logDbUid,logDbPwd);
			logSql = "UPDATE p2p_config SET value = ? WHERE key = ?";
			ps = connection.prepareStatement(logSql);
			ps.setString(1, value);
			ps.setString(2, property);
			ps.executeUpdate();
				    	
			
		} catch (SQLException e) {
			
			
			e.printStackTrace();
		} catch (Exception e) {
			
			e.printStackTrace();
		}
		
		
	}
	
	
	public String getConfig(String property) {
		
		PreparedStatement ps;
		String logSql = "";
		ResultSet rs = null;
		try {
			Connection connection = DriverManager.getConnection(logDbURL,logDbUid,logDbPwd);
			logSql = "SELECT value FROM p2p_config WHERE key = ?";
			ps = connection.prepareStatement(logSql);
			ps.setString(1, property);
			rs = ps.executeQuery();
			return rs.getString(0);
	    	
			
		} catch (SQLException e) {
			
			e.printStackTrace();
		} catch (Exception e) {
			
			e.printStackTrace();
		}
		
		return "";
	}

	public void copyConfigToDb() {
		
		Map<String, String> propMap = null;
		PreparedStatement ps;
		String logSql = "";
		
		String passKey = "ScholasticsETL12";
		appConfigPropFile = "config.properties";
		
				
		try {
			Connection connection = DriverManager.getConnection(logDbURL,logDbUid,logDbPwd);
			propMap = readPropertiesFileAsMap("config.properties", "=");
			
			logSql = "TRUNCATE TABLE p2p_config";
			ps = connection.prepareStatement(logSql);
			ps.executeUpdate();
			
			for (Map.Entry<String, String> entry : propMap.entrySet()) {
				
				logSql = "INSERT INTO p2p_config(property,value) "
						+ "VALUES(?,?)";
				
				ps = connection.prepareStatement(logSql);
				ps.setString(1, entry.getKey());
		    	ps.setString(2,EncryptionUtil.encryptAES(entry.getValue(), passKey));
				ps.executeUpdate();
			}
			
			connection.close();
			
		} catch (SQLException e) {
			
			e.printStackTrace();
		} catch (Exception e) {
			
			e.printStackTrace();
		}
	}
	
	public static Map<String, String> readPropertiesFileAsMap(String filename, String delimiter)
			throws Exception
			{
			  Map<String, String> map = new HashMap();
			  BufferedReader reader = new BufferedReader(new FileReader(filename));
			  String line;
			  Properties properties = new Properties();
			  File pf = new File(filename);
			  properties.load(new FileReader(pf));
			  String key,value;
			  
			  while ((line = reader.readLine()) != null)
			  {
			    if (line.trim().length()==0) continue;
			    if (line.charAt(0)=='#') continue;
			    int delimPosition = line.indexOf(delimiter);
			    key = line.substring(0, delimPosition-1).trim();
			    value = properties.getProperty(key).trim();
			    map.put(key, value);
			  }
			  reader.close();
			  return map;
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
			ResultSet rs = ps.getGeneratedKeys();
			if (rs != null && rs.next()) {
				key = rs.getLong(1);
			}

		} catch (Exception e) {
			e.getStackTrace();
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
				
			default:

				logSql = "UPDATE job_log SET job_status = ? WHERE job_id = ? ";
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
			System.exit(0);
		}
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
