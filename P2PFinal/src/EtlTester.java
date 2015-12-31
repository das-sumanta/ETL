import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class EtlTester {

	public static void main(String[] args) {

		Connection con = null;
		String aSQLScriptFilePath = "";
		String dbObjects[] = null;
		if (args.length == 0) {
			DataLoader dl;
			try {

				dl = new DataLoader();
				dl.createDbExtract(); 
				dl.doAmazonS3FileTransfer(); System.exit(0);

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
							scriptRunner.writeJobLog(dl.getRunID(),dbObjects[u],"Normal Mode","Success");
							break;

						} else {
							continue;
						}
					}
				}

			} catch (ClassNotFoundException e) {

				e.printStackTrace();

			} catch (IOException e) {

				e.printStackTrace();

			} catch (SQLException e) {

				e.printStackTrace();
			}

		} else {
			System.out.println("DataLoader running in mannual mode");
			String[] fileList = args[1].split(",");
							
			DataLoader dl;
			int runId = Integer.valueOf(args[2]);
			
			try {

				dl = new DataLoader("r",runId);
				//dl.createDbExtract('r', fileList);
				//dl.doAmazonS3FileTransfer('r', fileList);

				Properties properties = new Properties();
				File pf = new File("config.properties");
				properties.load(new FileReader(pf));
				String tmp = "";

				aSQLScriptFilePath = properties.getProperty("SQLScriptsPath");
				File folder = new File(aSQLScriptFilePath);
				File[] listOfFiles = folder.listFiles();

				//dbObjects = dl.getListOfDimensionsFacts();
				

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
				

			} catch (ClassNotFoundException e) {

				e.printStackTrace();

			} catch (IOException e) {

				e.printStackTrace();

			} catch (SQLException e) {

				e.printStackTrace();
			}

		}

	}

}
