import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class EtlTester {

	public static void main(String[] args) throws IOException, SQLException {
		
		Connection con = null;
		String aSQLScriptFilePath = "";
		String dbObjects[] = null;
		DataLoader dl=null;
		try {
			if (args.length == 0) {
				dl= new DataLoader();
				
				dl.createDbExtract(); 
				dl.doAmazonS3FileTransfer(); //System.exit(0);

				Properties properties = new Properties();
				File pf = new File("config.properties");
				properties.load(new FileReader(pf));
				String tmp = "";

				aSQLScriptFilePath = properties.getProperty("SQLScriptsPath");
				File folder = new File(aSQLScriptFilePath);
				File[] listOfFiles = folder.listFiles();

				dbObjects = dl.getListOfDimensionsFacts();


				for (int u = 0; u < dbObjects.length; u++) {
					for (int i = 0; i < listOfFiles.length; i++) {

						tmp =  dbObjects[u] + "_prestage_opn.sql"; 
						if (tmp.equalsIgnoreCase(listOfFiles[i].getName())) {

							// File file = listOfFiles[i];

							Class.forName(properties.getProperty("RSCLASS"));
							con = DriverManager.getConnection(
									properties.getProperty("RSDBURL"),
									properties.getProperty("RSUID"),
									properties.getProperty("RSPWD"));

							ScriptRunner scriptRunner = new ScriptRunner(con,
									false, true, dl.getRunID(), dbObjects[u],false);

							scriptRunner.runScript(new FileReader(
									aSQLScriptFilePath + File.separator
									+ listOfFiles[i].getName()));
							scriptRunner.writeJobLog(dl.getRunID(),dbObjects[u],"Normal Mode","Success"); //TODO dw stage needs to be captured
							break;

						} else {
							continue;
						}
					}
				}



			} else {
			System.out.println("DataLoader running in manual mode");
			String[] fileList = args[1].split(",");
			int runId = Integer.valueOf(args[2]);	
			dl = new DataLoader("r",runId);
			
			
		//	try {

				
				dl.createDbExtract('r', fileList);
				dl.doAmazonS3FileTransfer(/*'r',*/ fileList);

				Properties properties = new Properties();
				File pf = new File("config.properties");
				properties.load(new FileReader(pf));
				String tmp = "";

				aSQLScriptFilePath = properties.getProperty("SQLScriptsPath");
				File folder = new File(aSQLScriptFilePath);
				File[] listOfFiles = folder.listFiles();

				dbObjects = dl.getListOfDimensionsFacts();
				

				for (int u = 0; u < fileList.length; u++) {
					for (int i = 0; i < listOfFiles.length; i++) {

						tmp =  fileList[u] + "_prestage_opn.sql"; 
						if (tmp.equalsIgnoreCase(listOfFiles[i].getName())) {

							Class.forName(properties.getProperty("RSCLASS"));
							con = DriverManager.getConnection(
									properties.getProperty("RSDBURL"),
									properties.getProperty("RSUID"),
									properties.getProperty("RSPWD"));

							ScriptRunner scriptRunner = new ScriptRunner(con,
									false, true, dl.getRunID(), fileList[u],true);

							scriptRunner.runScript(new FileReader(
									aSQLScriptFilePath + File.separator
											+ listOfFiles[i].getName()));
							
							scriptRunner.writeJobLog(dl.getRunID(),fileList[u],"Re-run mode","Success");
							break;

						} else {
							continue;
						}
					}
				}
				
				dl.updateRunID(runId++);
				

			/*} catch (ClassNotFoundException e) {

				e.printStackTrace();

			} catch (IOException e) {

				e.printStackTrace();

			} catch (SQLException se) {
				System.out.println("Error !! Please check error message "
						+ se.getMessage());
				dl.writeLog("RunID " + dl.getRunID() + "Error !! Please check error message. " + se.getMessage(),
						"error","","Aplication Startup","db");
				System.exit(0);
			}*/

		}
		} catch (ClassNotFoundException e) {
			System.out.println("Error !! Please check error message "
					+ e.getMessage());
			dl.writeLog("RunID " + dl.getRunID()  + "Error !! Please check error message. "
					+ e.getMessage(), "error", "", "ScriptRunner Startup", "db");
			System.exit(0);
		} catch (IOException e) {

			e.printStackTrace();

		} catch (SQLException e) {
			dl.writeLog("RunID " + dl.getRunID() + " Error !! Please check error message. " + e.getMessage(),
					"error","","ScriptRunner Startup","db");
		}

	}

}
