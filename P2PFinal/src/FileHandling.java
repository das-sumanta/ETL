import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.Map;

import org.supercsv.io.CsvMapReader;
import org.supercsv.io.CsvMapWriter;
import org.supercsv.io.ICsvMapReader;
import org.supercsv.io.ICsvMapWriter;
import org.supercsv.prefs.CsvPreference;

public class FileHandling {
  public static void main(String args[])  {
	  ICsvMapReader mapReader = null;
      ICsvMapWriter mapWriter = null;
      
      
      try {
    	  URL website = new URL("https://system.sandbox.netsuite.com/core/media/media.nl?id=128431&c=3479023&h=4d7255abfdace6df4c13&_xt=.csv");
          ReadableByteChannel rbc = Channels.newChannel(website.openStream());
          FileOutputStream fos = new FileOutputStream("d:\\custom_form_tmp.csv");
          fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
    	  
    	  CsvPreference prefs = CsvPreference.STANDARD_PREFERENCE;
          mapReader = new CsvMapReader(new FileReader("d:\\custom_form_tmp.csv"), prefs);
          mapWriter = new CsvMapWriter(new FileWriter("d:\\custom_form.csv"), prefs);

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
          while( (row = mapReader.read(readHeader)) != null ) {

              // add your column with desired value
              row.put(timeHeader, "10");

              mapWriter.write(row, writeHeader);
          }

      } catch (IOException e) {
		// TODO Auto-generated catch block
		e.printStackTrace();
	}
      finally {
          if( mapReader != null ) {
              try {
				mapReader.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
          }
          if( mapWriter != null ) {
              try {
				mapWriter.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
          }
      }

  }
  }
