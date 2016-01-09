import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Properties;

public class EtlTester {

	public static void main(String[] args) throws IOException, SQLException {
		
		Connection con = null;
		String aSQLScriptFilePath = "";
		String dbObjects[] = null;
		DataLoader dl=null;
		Properties properties = new Properties();
		File pf = new File("config.properties");
		properties.load(new FileReader(pf));
		String FileExtractURLTableName = properties.getProperty("FileExtractURLTableName");
		
		try {
			if (args.length == 0) {
				Utility.applicationStart(false);
				
				String tmp = "";
				
				dl= new DataLoader();
				
				dl.createDbExtract(); 
				
				if(!FileExtractURLTableName.equals(""))
					dl.createDbExtractFromURL();
				
				dl.doAmazonS3FileTransfer(); 

				

				aSQLScriptFilePath = properties.getProperty("SQLScriptsPath");
				File folder = new File(aSQLScriptFilePath);
				File[] listOfFiles = folder.listFiles();

				dbObjects = dl.getListOfDimensionsFacts();


				for (int u = 0; u < dbObjects.length; u++) {
					for (int i = 0; i < listOfFiles.length; i++) {

						tmp =  dbObjects[u] + "_prestage_opn.sql"; 
						long jobId = Utility.dbObjectJobIdMap.get(dbObjects[u]);
						if (tmp.equalsIgnoreCase(listOfFiles[i].getName())) {

							
							Class.forName(properties.getProperty("RSCLASS"));
							con = DriverManager.getConnection(
									properties.getProperty("RSDBURL"),
									properties.getProperty("RSUID"),
									properties.getProperty("RSPWD"));

							ScriptRunner scriptRunner = new ScriptRunner(con,
									false, true, Utility.runID, dbObjects[u],false);

							scriptRunner.runScript(new FileReader(
									aSQLScriptFilePath + File.separator
									+ listOfFiles[i].getName()),jobId);
						//	scriptRunner.writeJobLog(Utility.runID,dbObjects[u],"Normal Mode","Success"); 
							Utility.writeJobLog(jobId, "COMPLETED",
									new SimpleDateFormat("YYYY-MM-DD HH:mm:ss").format(Calendar
											.getInstance().getTime()));
							break;

						} else {
							continue;
						}
					}
				}



			} else {
				
			System.out.println("DataLoader running in manual mode");
			String[] fileList = args[1].split(",");
			Utility.runID = Integer.valueOf(args[2]);
			Utility.applicationStart(true);
			
			dl = new DataLoader("r",Utility.runID);
			
			
		
				
				dl.createDbExtract('r', fileList);
				
				if(!FileExtractURLTableName.isEmpty()) {
					String[] arr= FileExtractURLTableName.split(",");
					for(String urlDimension:arr) {
						if(Arrays.asList(fileList).contains(urlDimension)) {
							dl.createDbExtractFromURL();
						}
					}
				}
				dl.doAmazonS3FileTransfer(/*'r',*/ fileList);


				String tmp = "";

				aSQLScriptFilePath = properties.getProperty("SQLScriptsPath");
				File folder = new File(aSQLScriptFilePath);
				File[] listOfFiles = folder.listFiles();

				dbObjects = dl.getListOfDimensionsFacts();
				

				for (int u = 0; u < fileList.length; u++) {
					for (int i = 0; i < listOfFiles.length; i++) {

						tmp =  fileList[u] + "_prestage_opn.sql"; 
						long jobId = Utility.dbObjectJobIdMap.get(fileList[u]);
						if (tmp.equalsIgnoreCase(listOfFiles[i].getName())) {

							Class.forName(properties.getProperty("RSCLASS"));
							con = DriverManager.getConnection(
									properties.getProperty("RSDBURL"),
									properties.getProperty("RSUID"),
									properties.getProperty("RSPWD"));

							ScriptRunner scriptRunner = new ScriptRunner(con,
									false, true, Utility.runID, fileList[u],true);

							scriptRunner.runScript(new FileReader(
									aSQLScriptFilePath + File.separator
											+ listOfFiles[i].getName()),jobId);
							
							Utility.writeJobLog(jobId, "COMPLETED",
									new SimpleDateFormat("YYYY-MM-DD HH:mm:ss").format(Calendar
											.getInstance().getTime()));
							break;

						} else {
							continue;
						}
					}
				}
				

		}
		} catch (ClassNotFoundException e) {
			System.out.println("Error !! Please check error message "
					+ e.getMessage());
			dl.writeLog("RunID " + Utility.runID  + "Error !! Please check error message. "
					+ e.getMessage(), "error", "", "ScriptRunner Startup", "db");
			System.exit(0);
		} catch (IOException e) {

			e.printStackTrace();

		} catch (SQLException e) {
			dl.writeLog("RunID " + Utility.runID + " Error !! Please check error message. " + e.getMessage(),
					"error","","ScriptRunner Startup","db");
		}

	}

}
