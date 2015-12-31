import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLConnection;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.supercsv.io.CsvListReader;
import org.supercsv.io.CsvListWriter;
import org.supercsv.prefs.CsvPreference;

public class getFromURL {

	public static void main(String[] args) {

		SimpleDateFormat sdf = new SimpleDateFormat("dd-MM-yyyy");
		Date curDate = new Date();
		String strDate = sdf.format(curDate);
		
		URL url;
		String urlExtractLocationLocal = System.getProperty("user.dir") + File.separator
				+ "URL_DB_Extracts" + File.separator + strDate;
		try {
			new File(urlExtractLocationLocal).mkdirs();
			// get URL content
			url = new URL("https://system.sandbox.netsuite.com/core/media/media.nl?id=128431&c=3479023&h=4d7255abfdace6df4c13&_xt=.csv");
			URLConnection conn = url.openConnection();

			// open the stream and put it into BufferedReader
			BufferedReader br = new BufferedReader(
                               new InputStreamReader(conn.getInputStream()));

			String inputLine;

			//save to this filename
			String fileName = urlExtractLocationLocal + File.separator + "CUSTOM_FORM_"+strDate+"_tmp.csv";
			String finalFileName = urlExtractLocationLocal + File.separator + "CUSTOM_FORM_"+strDate+".csv";
			File file = new File(fileName);

			if (!file.exists()) {
				file.createNewFile();
			}

			//use FileWriter to write file
			FileWriter fw = new FileWriter(file.getAbsoluteFile());
			BufferedWriter bw = new BufferedWriter(fw);

			while ((inputLine = br.readLine()) != null) {
				bw.write(inputLine);
			}

			bw.close();
			br.close();
			CsvListReader reader = new CsvListReader(new FileReader(fileName), CsvPreference.STANDARD_PREFERENCE);
			CsvListWriter writer = new CsvListWriter(new FileWriter(finalFileName), CsvPreference.STANDARD_PREFERENCE);
			List<String> columns;
			while ((columns = reader.read()) != null) {
			    System.out.println("Input: " + columns);
			    // Add new columns
			    columns.add(1, "Column_2");
			    columns.add("Last_column");

			    System.out.println("Output: " + columns);
			    writer.write(columns);
			}
			reader.close();
			writer.close();

			System.out.println("Done");

		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

}
