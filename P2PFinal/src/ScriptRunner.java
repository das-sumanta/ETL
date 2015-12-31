
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;


public class ScriptRunner {

    public static final String DEFAULT_DELIMITER = ";";
    public static final String PL_SQL_BLOCK_SPLIT_DELIMITER = "+";
    public static final String PL_SQL_BLOCK_END_DELIMITER = "#";

    private final boolean autoCommit, stopOnError;
    private final Connection connection;
    private String delimiter = ScriptRunner.DEFAULT_DELIMITER;
    private final PrintWriter out, err;
    private String errorSql;
    private PreparedStatement ps;
    private String errorCode;
    private int runID;
    private String entity;
    private boolean manualMode;
   
    
	/* To Store any 'SELECT' queries output */
    private List<Table> tableList;
	
	/* To Store any SQL Queries output except 'SELECT' SQL */
    private List<String> sqlOutput;

    public ScriptRunner(final Connection connection, final boolean autoCommit, final boolean stopOnError,int runID,String entity, boolean isManualMode) {
        if (connection == null) {
            throw new RuntimeException("ScriptRunner requires an SQL Connection");
        }
		
        this.connection = connection;
        this.autoCommit = autoCommit;
        this.stopOnError = stopOnError;
        this.out = new PrintWriter(System.out);
        this.err = new PrintWriter(System.err);
        this.runID = runID;
        this.entity = entity;
        this.manualMode = isManualMode;
        tableList = new ArrayList<Table>();
        sqlOutput = new ArrayList<String>();
        errorCode = "";

    }

    public void runScript(final Reader reader) throws SQLException, IOException {
        final boolean originalAutoCommit = this.connection.getAutoCommit();
        try {
            if (originalAutoCommit != this.autoCommit) {
                this.connection.setAutoCommit(this.autoCommit);
            }
            this.runScript(this.connection, reader);
        } finally {
            this.connection.setAutoCommit(originalAutoCommit);
        }
    }

    private void runScript(final Connection conn, final Reader reader) throws SQLException, IOException {
        StringBuffer command = null;
        String temp = "";
        Table table = null;
       
        try {
            final LineNumberReader lineReader = new LineNumberReader(reader);
            String line = null;
            while ((line = lineReader.readLine()) != null) {
                if (command == null) {
                    command = new StringBuffer();
                }

                if (table == null) {
                    table = new Table();
                }

                String trimmedLine = line.trim();
                
                              
               if (trimmedLine.startsWith("/*") && trimmedLine.endsWith("*/")) {
            	   
            	   errorCode = trimmedLine.substring(trimmedLine.indexOf("*") + 1, trimmedLine.lastIndexOf("*")).trim();
            	   
            	          	   
               }
               
				// Interpret SQL Comment & Some statement that are not executable
                if (trimmedLine.startsWith("--")
                        || trimmedLine.startsWith("//")
                        || trimmedLine.startsWith("#")
                        || trimmedLine.startsWith("/*")
                        || trimmedLine.toLowerCase().startsWith("rem inserting into")
                        || trimmedLine.toLowerCase().startsWith("set define off")) {

                    // do nothing...
                } else if (trimmedLine.endsWith(this.delimiter) || trimmedLine.endsWith(PL_SQL_BLOCK_END_DELIMITER)) { // Line is end of statement
                    
                    // Append
                    if (trimmedLine.endsWith(this.delimiter)) {
                        command.append(line.substring(0, line.lastIndexOf(this.delimiter)));
                        command.append(" ");
 
                    } else if (trimmedLine.endsWith(PL_SQL_BLOCK_END_DELIMITER)) {
                        command.append(line.substring(0, line.lastIndexOf(PL_SQL_BLOCK_END_DELIMITER)));
                        command.append(" ");
                        
                    }

                    Statement stmt = null;
                    ResultSet rs = null;
                    try {
                        stmt = conn.createStatement();
                        boolean hasResults = false;
                        if (this.stopOnError) { 
                        	System.out.println("Executing query ->" + command.toString());
                            hasResults = stmt.execute(command.toString());
                            //writeDBLog(runID,stmt.getUpdateCount() + " row(s) affected.",entity, this.errorCode,"Info" );
                        } else {
                            try {
                            	System.out.println("Executing query ->" + command.toString());
                            	stmt.execute(command.toString());
                            	writeDBLog(runID,stmt.getUpdateCount() + " row(s) affected.",entity, this.errorCode,"Info" );
                            } catch (final SQLException e) {
                                e.fillInStackTrace();
                                err.println("Error executing SQL Command: \"" + command + "\"");
                                err.println(e);
                                err.flush();
                                writeDBLog(runID, e.getMessage(),entity, this.errorCode,"Error" );
                                throw e;
                            }
                        }

                        rs = stmt.getResultSet();
                        if (hasResults && rs != null) {

                            List<String> headerRow = new ArrayList<String>();
                            List<List<String>> toupleList = new ArrayList<List<String>>();

                            // Print & Store result column names
                            final ResultSetMetaData md = rs.getMetaData();
                            final int cols = md.getColumnCount();
                            for (int i = 0; i < cols; i++) {
                                final String name = md.getColumnLabel(i + 1);
                                out.print(name + "\t");

                                headerRow.add(name);
                            }
							
                            table.setHeaderRow(headerRow);

                            out.println("");
                            out.println(StringUtils.repeat("---------", md.getColumnCount()));
                            out.flush();

                            // Print & Store result rows
                            while (rs.next()) {
                                List<String> touple = new ArrayList<String>();
                                for (int i = 1; i <= cols; i++) {
                                    final String value = rs.getString(i);
                                    out.print(value + "\t");

                                    touple.add(value);
                                }
                                out.println("");

                                toupleList.add(touple);
                            }
                            out.flush();

                            table.setToupleList(toupleList);
                            this.tableList.add(table);
                            table = null;
                        } else {
                            sqlOutput.add(stmt.getUpdateCount() + " row(s) affected.");

                            out.println(stmt.getUpdateCount() + " row(s) affected.");
                            out.flush();
                            if(!errorCode.equals("")) {
                            	writeDBLog(runID,stmt.getUpdateCount() + " row(s) affected.",entity, this.errorCode,"Info" );
                            	
                                errorCode = "";
                            }
                            
                        }
                        command = null;
                                        	
                    } finally {
                    	
                        if (rs != null) {
                            try {
                                rs.close();
                            } catch (final Exception e) {
                                err.println("Failed to close result: " + e.getMessage());
                                err.flush();
                            }
                        }
                        if (stmt != null) {
                            try {
                                stmt.close();
                                
                            } catch (final Exception e) {
                                err.println("Failed to close statement: " + e.getMessage());
                                err.flush();
                            }
                        }
                    }
                } else if (trimmedLine.endsWith(PL_SQL_BLOCK_SPLIT_DELIMITER)) {
                    command.append(line.substring(0, line.lastIndexOf(this.PL_SQL_BLOCK_SPLIT_DELIMITER)));
                    command.append(" ");
                } else { // Line is middle of a statement

                    // Append
                    command.append(line);
                    command.append(" ");
                    
                }
            }
            if (!this.autoCommit) {
                conn.commit();
            }
        } catch (final SQLException e) {
            conn.rollback();
            e.fillInStackTrace();
            err.println("Error executing SQL Command: \"" + command + "\"");
            err.println(e);
            err.flush();
            writeDBLog(runID, e.getMessage(),entity, this.errorCode,"Error" );
            writeJobLog(runID,entity,(manualMode)?"Rerun Mode":"Normal Mode","Error");
            throw e;
        } catch (final IOException e) {
            e.fillInStackTrace();
            err.println("Error reading SQL Script.");
            err.println(e);
            err.flush();
            throw e;
        }
    }
    
    
    public void writeDBLog(int runID,String errorDesc,String entity,String errStg,String msgTyp) throws SQLException {
    	
    	Calendar calendar = Calendar.getInstance();
    	Timestamp currentTimestamp = new java.sql.Timestamp(calendar.getTime().getTime());
    	
    	errorSql = "INSERT INTO dw_prestage.message_log(runid,message_desc,target_table,message_stage,message_type,message_timestamp) "
				+ "VALUES(?,?,?,?,?,?)";
		
		ps = connection.prepareStatement(errorSql);
		ps.setInt(1, runID);
		ps.setString(2,errorDesc);
		ps.setString(3, entity);
		ps.setString(4, errStg);
		ps.setString(5, msgTyp);
		ps.setTimestamp(6, currentTimestamp);
		ps.executeUpdate();
		connection.commit();
		
    }
    
    public void writeJobLog(int runID,String entity,String run_mode,String job_status) throws SQLException {
    	
    	Calendar calendar = Calendar.getInstance();
    	Timestamp currentTimestamp = new java.sql.Timestamp(calendar.getTime().getTime());
    	String logSql = "";
    	
    	logSql = "INSERT INTO dw_prestage.job_log(runid,entity,run_mode,job_status,job_timestamp) "
				+ "VALUES(?,?,?,?,?)";
		
		ps = connection.prepareStatement(logSql);
		ps.setInt(1, runID);
		ps.setString(2,entity);
		ps.setString(3, run_mode);
		ps.setString(4, job_status);
		ps.setTimestamp(5, currentTimestamp);
		ps.executeUpdate();
		connection.commit();
		
    }

    /**
     * @return the tableList
     */
    public List<Table> getTableList() {
        return tableList;
    }

    /**
     * @param tableList the tableList to set
     */
    public void setTableList(List<Table> tableList) {
        this.tableList = tableList;
    }

    /**
     * @return the sqlOutput
     */
    public List<String> getSqlOutput() {
        return sqlOutput;
    }

    /**
     * @param sqlOutput the sqlOutput to set
     */
    public void setSqlOutput(List<String> sqlOutput) {
        this.sqlOutput = sqlOutput;
    }
}
