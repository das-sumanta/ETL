

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.Properties;

public class RelationerDataLoader_new {

	private String appConfigPropFile;
	private String msUid;
	private String msPwd;
	private String conStrMS;
	private Connection msCon;
	private String baseTbl;
	private String refTbl;
	private String finalTbl;
	private int hierarchyDepth;
	private String redShiftSchemaNamePreStage;
	private String redShiftSchemaNameStage;
	private String redShiftSchemaNameFinal;
	
	public RelationerDataLoader_new() {

		appConfigPropFile = "config.properties";
		Properties properties = new Properties();
		File pf = new File(appConfigPropFile);
		try {
			properties.load(new FileReader(pf));

		} catch (IOException e) {

			System.out.println("Error loading properties file.\n"
					+ e.getMessage());
		}

		msUid = properties.getProperty("RSUID");
		msPwd = properties.getProperty("RSPWD");
		conStrMS = properties.getProperty("RSDBURL");
		baseTbl = properties.getProperty("TBLBaseEmp");
		refTbl = properties.getProperty("TBLRefEmp");
		finalTbl = properties.getProperty("TBLFinalEMP");
		redShiftSchemaNamePreStage = properties.getProperty("RSSCHEMAPRESTAGE");
		redShiftSchemaNameStage = properties.getProperty("RSSCHEMASTAGE");
		redShiftSchemaNameFinal = properties.getProperty("RSSCHEMA");
		
			
		try {
			Class.forName("com.amazon.redshift.jdbc41.Driver");
			Properties props = new Properties();

			props.setProperty("user", msUid);
			props.setProperty("password", msPwd);
			msCon = DriverManager.getConnection(conStrMS, props);
			
			createAppTmpTbl();
			populateAppTmpTbl();
			
		} catch (ClassNotFoundException e) {
			
			e.printStackTrace();
		} catch (SQLException e) {
			
			e.printStackTrace();
		}

	}

	public void processResult() throws SQLException {
		String sql = "";
		PreparedStatement ps = null;
		int lvl = 0, tot  = 0; 
		System.out.println("Inserting top level employees to the organization database..");
		
			try {
												
				sql = "INSERT INTO "+ redShiftSchemaNamePreStage + "." 
						+ baseTbl + "(employee_id, employee_name, employee_job_title, "
						+ "supervisor_id, supervisor_name, supervisor_job_title,level) "
						+ "SELECT employee_id, employee_name, job_title, supervisor_id, supervisor_name, supervisor_job_title, ?"
						+ " FROM "+ redShiftSchemaNamePreStage + "." + refTbl
					+ " WHERE (SUPERVISOR_ID is null)";
				ps = msCon.prepareStatement(sql);
				ps.setInt(1, 0);
				tot = ps.executeUpdate();
				System.out.println("No of employess added to this level are " + tot);
				

			} catch (SQLException e) {
				
				System.out.println("Error!!\n" + e.getMessage());
				msCon.rollback();
			}

		
		
		addEmployeeBasedOnLavel(lvl + 1, lvl);
		addEmployeeBasedOnQLavel();
		
		System.out.println("All employees are added to the database. Program will now exit.");
		
		msCon.close();

	}

	public void addEmployeeBasedOnLavel(int nxtLvl, int prvLvl) throws SQLException {

		String sql = "";
		PreparedStatement ps = null;
		int res = 0, lvl = prvLvl;
		//Savepoint savepoint2 = null;
		
		 
		System.out.println("Inserting employees of level " + nxtLvl + " to the organization database..");
		sql = "INSERT INTO "+ redShiftSchemaNamePreStage + "." 
				+ baseTbl
				+ " (employee_id,employee_name,employee_job_title,supervisor_id,supervisor_name,supervisor_job_title,level) "
				+ "SELECT b.employee_id,b.employee_name,b.job_title,b.supervisor_id,b.supervisor_name,b.supervisor_job_title,? FROM "
				+ redShiftSchemaNamePreStage + "." 
				+ baseTbl + " a," + redShiftSchemaNamePreStage + "." + refTbl + " b WHERE a.LEVEL = ? and a.EMPLOYEE_ID = b.SUPERVISOR_ID";

		try {

			//savepoint2 = msCon.setSavepoint("Before_Insertion_SUB");
			ps = msCon.prepareStatement(sql);
			ps.setInt(1, nxtLvl);
			ps.setInt(2, prvLvl);
			res = ps.executeUpdate();
			
			System.out.println("No of employess added to the level:- " + nxtLvl + " are " + res);
			

			if (res > 0) {
				lvl++;
				nxtLvl = lvl + 1;
				prvLvl = lvl;
				addEmployeeBasedOnLavel(nxtLvl, prvLvl);
				

			} else {
				hierarchyDepth = prvLvl;
				return;
			}

		} catch (SQLException e) {

			System.out.println("Error!!" + System.getProperty("line.separator")
					+ e.getMessage());
			
			msCon.rollback();
			
			System.exit(0);
		}

	}
	
	public void addEmployeeBasedOnQLavel() throws SQLException {

		String sql = "";
		PreparedStatement ps = null;
		int res = 0;
		//Savepoint savepoint3 = null;
					
		try {
			//savepoint3 = msCon.setSavepoint("Before_Insertion_FINAL");
			
			//LOOP THROUGH THE DEPTH OF THE EMPLOYEE-BOSS HIERERCHY 
			
			for(int i = 1; i <= hierarchyDepth; i++) {
				
				if(i == 1){
					
					sql = "INSERT INTO "+ redShiftSchemaNamePreStage + "." + finalTbl + " (employee_id,employee_name,employee_job_title,"
							+ "supervisor_id,supervisor_name,supervisor_job_title,level,q_level) "
							+ "SELECT employee_id, employee_name, employee_job_title,supervisor_id,supervisor_name,supervisor_job_title, ?, ? "
							+ " FROM " + redShiftSchemaNamePreStage + "." + baseTbl + " WHERE LEVEL = ?";
													
					ps = msCon.prepareStatement(sql);
					ps.setInt(1, i);
					ps.setInt(2, i-1);
					ps.setInt(3, i);
					System.out.println(ps);
					res = ps.executeUpdate();
					System.out.println("No. of employess added to the level " + i + " are " + res);
					
				} else {
					
					sql = "INSERT INTO "+ redShiftSchemaNamePreStage + "." + finalTbl + " (employee_id,employee_name,employee_job_title,"
							+ "supervisor_id,supervisor_name,supervisor_job_title,level,q_level) "
							+ "SELECT employee_id, employee_name, employee_job_title,supervisor_id,supervisor_name,supervisor_job_title, ?, ? "
							+ " FROM " + redShiftSchemaNamePreStage + "." + baseTbl + " WHERE LEVEL = ?";
													
					ps = msCon.prepareStatement(sql);
					ps.setInt(1, 1);
					ps.setInt(2, i);
					ps.setInt(3, i);
					System.out.println(ps);
					res = ps.executeUpdate();
					System.out.println("No. of employess added to the level " + i +" are " + res);
					
				}
				
				for(int j = 1; j < i; j++){
					
					sql = "INSERT INTO "+ redShiftSchemaNamePreStage + "." + finalTbl + " (employee_id,employee_name,employee_job_title,"
							+ "supervisor_id,supervisor_name,supervisor_job_title,level,q_level) "
							+ " SELECT b.employee_id, b.employee_name,b.employee_job_title,a.supervisor_id,a.supervisor_name,a.supervisor_job_title, ? , ? FROM "
							+ "(SELECT employee_id,employee_name,employee_job_title,supervisor_id,supervisor_name,supervisor_job_title,level FROM "
							+ redShiftSchemaNamePreStage + "." + baseTbl + " WHERE level = ?) a, "
							+ "(SELECT employee_id,employee_name,employee_job_title,supervisor_id,supervisor_name,supervisor_job_title,level,q_level"
							+ " FROM "+ redShiftSchemaNamePreStage + "." + finalTbl + " WHERE Q_LEVEL = ? and LEVEL = ?) b "
							+ "WHERE a.EMPLOYEE_ID = b.SUPERVISOR_ID";
					
					ps = msCon.prepareStatement(sql);
					ps.setInt(1, j+1);
					ps.setInt(2, i);
					ps.setInt(3, i-j);
					ps.setInt(4, i);
					ps.setInt(5, j);
					System.out.println(ps);
					res = ps.executeUpdate();
					System.out.println("No. of employess added to this level are " + res);
					

				}
			}
			sql = "INSERT INTO "+ redShiftSchemaNamePreStage + "." + finalTbl + " (employee_id,employee_name,employee_job_title,supervisor_id,supervisor_name,supervisor_job_title,level) "
					+ "SELECT DISTINCT employee_id , employee_name, employee_job_title,employee_id , employee_name, employee_job_title, 0 FROM " + redShiftSchemaNamePreStage + "." + baseTbl;
			
			ps = msCon.prepareStatement(sql);
			res = ps.executeUpdate();
			System.out.println("No. of employess added to this level are " + res);
			


		} catch (SQLException e) {

			System.out.println("Error!!" + System.getProperty("line.separator")
					+ e.getMessage());
			
			msCon.rollback();
			
			System.exit(0);
		}

	}

	public ResultSet getEmployeeList(int mgrID) {

		String sql;
		Statement statement;
		ResultSet resultSet = null;

		if (mgrID == 0)
			sql = "SELECT * FROM "+ redShiftSchemaNamePreStage + "." + refTbl
					+ " WHERE (SUPERVISOR_ID is null)";
		else
			sql = "";

		try {

			statement = msCon.createStatement();
			resultSet = statement.executeQuery(sql);
			

		} catch (SQLException e) {

			System.out.println("Error raised during executing query " + sql
					+ "\n" + e.getMessage());
		}
		
		return resultSet;

	}

	public int recordCount(ResultSet result) {
		int totalCount = 0;

		try {
			result.last();
			totalCount = result.getRow();
			result.beforeFirst();
			return totalCount;

		} catch (SQLException e) {

			System.out.println("Blank or null resultset passed!!\n"
					+ e.getMessage());
		}

		return totalCount;
	}
	
	public void createAppTmpTbl() {
		
		Statement stmt = null;
		String sql;
		System.out.println("Temporary Table Creation is started...");
		try {
			stmt = msCon.createStatement();
			sql = "DROP TABLE IF EXISTS " + redShiftSchemaNamePreStage + "." + refTbl;
			stmt.execute(sql);
			
			
			System.out.println("Creting Table " + refTbl + "...");
			
			sql = "CREATE TABLE " + redShiftSchemaNamePreStage + "." + refTbl + "(employee_id integer,"
					+ "employee_name varchar(500), job_title varchar(500), supervisor_id integer, "
					+ "supervisor_name varchar(500), supervisor_job_title varchar(500));";
			stmt.execute(sql);
			System.out.println("Done..");
			

			sql = "DROP TABLE IF EXISTS " + redShiftSchemaNamePreStage + "." + baseTbl;
			stmt.execute(sql);
			
			
			System.out.println("Creting Table " + baseTbl + "...");
			
			sql = "CREATE TABLE " + redShiftSchemaNamePreStage + "." + baseTbl + "(employee_id integer,"
					+ "employee_name varchar(500), employee_job_title varchar(500), supervisor_id integer, "
					+ "supervisor_name varchar(500), supervisor_job_title varchar(500), level integer);";
			stmt.execute(sql);
			System.out.println("Done..");
			
			
			sql = "DROP TABLE IF EXISTS " + redShiftSchemaNamePreStage + "." + finalTbl;
			stmt.execute(sql);
			
			
			System.out.println("Creting Table " + finalTbl + "...");
			
			sql = "CREATE TABLE " + redShiftSchemaNamePreStage + "." + finalTbl + "(employee_id integer,"
					+ "employee_name varchar(500), employee_job_title varchar(500), supervisor_id integer, "
					+ "supervisor_name varchar(500), supervisor_job_title varchar(500), level integer, q_level integer);";
			stmt.execute(sql);
			System.out.println("Done..");
			
			
		} catch (SQLException e) {
			
			e.printStackTrace();
		}
		
	}
	
	public void populateAppTmpTbl() {
		
		Statement stmt = null;
		String sql;
		System.out.println("Populating table " + refTbl + " with the data of employees table");
		try {
			stmt = msCon.createStatement();
			
			sql = "INSERT INTO " + redShiftSchemaNamePreStage + "." + refTbl 
					+ " SELECT a.employee_id , a.full_name as employee_name , a.jobtitle, "
					+ "a.supervisor_id , b.full_name as supervisor_name, b.jobtitle"
					+ " FROM "+ redShiftSchemaNamePreStage + ".employees a, "+ redShiftSchemaNamePreStage + ".employees b "
					+ " WHERE a.supervisor_id = b.employee_id(+);";
			
			int res = stmt.executeUpdate(sql);
			System.out.println(res + " rows are populated.");
			
			
		} catch(SQLException e) {
			e.printStackTrace();
		}
		
		
	}
	
	public void writeQueries(String str) {
		
		try {
			PrintWriter outputStream = new PrintWriter(new FileOutputStream("c:\\query.txt", true));
			outputStream.append(str);
			outputStream.println();
			outputStream.flush();
			outputStream.close();
		} catch (FileNotFoundException e) {
			
			e.printStackTrace();
		}
	}

	public static void main(String args[]) {

		RelationerDataLoader_new j1 = new RelationerDataLoader_new();
		try {
			j1.processResult();
			//j1.addEmployeeBasedOnQLavel();
			
		} catch (SQLException e) {

			e.printStackTrace();
		}
	}

}
