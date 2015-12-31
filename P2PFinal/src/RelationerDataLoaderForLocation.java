

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.Properties;

public class RelationerDataLoaderForLocation {

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
	
	public RelationerDataLoaderForLocation() {

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
		baseTbl = properties.getProperty("TBLBaseLoc");
		refTbl = properties.getProperty("TBLRefLoc");
		finalTbl = properties.getProperty("TBLFinalLoc");
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
		System.out.println("Inserting prime locations to the organization database..");
		
			try {
												
				sql = "INSERT INTO "+ redShiftSchemaNamePreStage + "." 
						+ baseTbl + "(location_id, location, address, "
						+ "parent_id, parent_location, parent_loc_address) "
						+ "SELECT location_id, location, address, parent_id, parent_location, parent_loc_address"
						+ " FROM "+ redShiftSchemaNamePreStage + "." + refTbl
					+ " WHERE (parent_id is null)";
				ps = msCon.prepareStatement(sql);
				//ps.setInt(1, null);
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
		
		 
		System.out.println("Inserting next level locations " + nxtLvl + " to the organization database..");
		if(prvLvl == 0) {
		sql = "INSERT INTO "+ redShiftSchemaNamePreStage + "." 
				+ baseTbl
				+ " (location_id,location,address,parent_id,parent_location,parent_loc_address,level) "
				+ "SELECT b.location_id,b.location,b.address,b.parent_id,b.parent_location,b.parent_loc_address,1 FROM "
				+ redShiftSchemaNamePreStage + "." 
				+ baseTbl + " a," + redShiftSchemaNamePreStage + "." + refTbl + " b WHERE a.LEVEL is null and a.location_id = b.parent_id";
		
		ps = msCon.prepareStatement(sql);
		res = ps.executeUpdate();
		
		
		} else {
			
			sql = "INSERT INTO "+ redShiftSchemaNamePreStage + "." 
					+ baseTbl
					+ " (location_id,location,address,parent_id,parent_location,parent_loc_address,level) "
					+ "SELECT b.location_id,b.location,b.address,b.parent_id,b.parent_location,b.parent_loc_address,? FROM "
					+ redShiftSchemaNamePreStage + "." 
					+ baseTbl + " a," + redShiftSchemaNamePreStage + "." + refTbl + " b WHERE a.LEVEL = ? and a.location_id = b.parent_id";
			
			ps = msCon.prepareStatement(sql);
			ps.setInt(1, nxtLvl);
			ps.setInt(2, prvLvl);
			res = ps.executeUpdate();
			
		}

		try {

						
			System.out.println("No of locations added to the level:- " + nxtLvl + " are " + res);
			

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
			
			//LOOP THROUGH THE DEPTH OF THE LOCATION-PARENT LOCATION HIERERCHY 
			
			for(int i = 1; i <= hierarchyDepth; i++) {
				
				if(i == 1){
					
					sql = "INSERT INTO "+ redShiftSchemaNamePreStage + "." + finalTbl + " (location_id,location,address,"
							+ "parent_id,parent_location,parent_loc_address,level,q_level) "
							+ "SELECT location_id, location, address,parent_id,parent_location,parent_loc_address, ?, ? "
							+ " FROM " + redShiftSchemaNamePreStage + "." + baseTbl + " WHERE LEVEL = ?";
													
					ps = msCon.prepareStatement(sql);
					ps.setInt(1, i);
					ps.setInt(2, i-1);
					ps.setInt(3, i);
					System.out.println(ps);
					res = ps.executeUpdate();
					System.out.println("No. of locations added to the level " + i + " are " + res);
					
				} else {
					
					sql = "INSERT INTO "+ redShiftSchemaNamePreStage + "." + finalTbl + " (location_id,location,address,"
							+ "parent_id,parent_location,parent_loc_address,level,q_level) "
							+ "SELECT location_id, location, address,parent_id,parent_location,parent_loc_address, ?, ? "
							+ " FROM " + redShiftSchemaNamePreStage + "." + baseTbl + " WHERE LEVEL = ?";
													
					ps = msCon.prepareStatement(sql);
					ps.setInt(1, 1);
					ps.setInt(2, i);
					ps.setInt(3, i);
					System.out.println(ps);
					res = ps.executeUpdate();
					System.out.println("No. of locations added to the level " + i +" are " + res);
					
				}
				
				for(int j = 1; j < i; j++){
					
					sql = "INSERT INTO "+ redShiftSchemaNamePreStage + "." + finalTbl + " (location_id,location,address,"
							+ "parent_id,parent_location,parent_loc_address,level,q_level) "
							+ " SELECT b.location_id, b.location,b.address,a.parent_id,a.parent_location,a.parent_loc_address, ? , ? FROM "
							+ "(SELECT location_id, location,address,parent_id,parent_location,parent_loc_address,level FROM "
							+ redShiftSchemaNamePreStage + "." + baseTbl + " WHERE level = ?) a, "
							+ "(SELECT location_id, location,address,parent_id,parent_location,parent_loc_address,level,q_level"
							+ " FROM "+ redShiftSchemaNamePreStage + "." + finalTbl + " WHERE Q_LEVEL = ? and LEVEL = ?) b "
							+ "WHERE a.location_id = b.parent_id";
					
					ps = msCon.prepareStatement(sql);
					ps.setInt(1, j+1);
					ps.setInt(2, i);
					ps.setInt(3, i-j);
					ps.setInt(4, i);
					ps.setInt(5, j);
					System.out.println(ps);
					res = ps.executeUpdate();
					System.out.println("No. of locations added to this level are " + res);
					

				}
			}
			sql = "INSERT INTO "+ redShiftSchemaNamePreStage + "." + finalTbl + " (location_id, location,address,parent_id,parent_location,parent_loc_address,level) "
					+ "SELECT DISTINCT location_id , location, address,location_id , location, address, 0 FROM " + redShiftSchemaNamePreStage + "." + baseTbl;
			
			ps = msCon.prepareStatement(sql);
			res = ps.executeUpdate();
			System.out.println("No. of locations added to this level are " + res);
			


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
			sql = "SELECT * FROM " + refTbl
					+ " WHERE (SUPERVISOR_ID is null) or (SUPERVISOR_ID = 0)";
		else
			sql = "";

		try {

			statement = msCon.createStatement(
					ResultSet.TYPE_SCROLL_INSENSITIVE,
					ResultSet.CONCUR_READ_ONLY);
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
			
			sql = "CREATE TABLE " + redShiftSchemaNamePreStage + "." + refTbl + "(location_id integer,"
					+ "location varchar(800), address varchar(1000), parent_id integer,parent_location varchar(800),parent_loc_address varchar(1000) )";
					
			stmt.execute(sql);
			System.out.println("Done..");
			

			sql = "DROP TABLE IF EXISTS " + redShiftSchemaNamePreStage + "." + baseTbl;
			stmt.execute(sql);
			
			
			System.out.println("Creting Table " + baseTbl + "...");
			
			sql = "CREATE TABLE " + redShiftSchemaNamePreStage + "." + baseTbl + "(location_id integer,"
					+ "location varchar(800), address varchar(1000), parent_id integer,parent_location varchar(800),parent_loc_address varchar(1000), level integer);";
			stmt.execute(sql);
			System.out.println("Done..");
			
			
			sql = "DROP TABLE IF EXISTS " + redShiftSchemaNamePreStage + "." + finalTbl;
			stmt.execute(sql);
			
			
			System.out.println("Creting Table " + finalTbl + "...");
			
			sql = "CREATE TABLE " + redShiftSchemaNamePreStage + "." + finalTbl + "(location_id integer,"
					+ "location varchar(800), address varchar(1000), parent_id integer, parent_location varchar(800),parent_loc_address varchar(1000), level integer, q_level integer);";
			
			stmt.execute(sql);
			System.out.println("Done..");
			
			
		} catch (SQLException e) {
			
			e.printStackTrace();
		}
		
	}

public void populateAppTmpTbl() {
	
	Statement stmt = null;
	String sql;
	System.out.println("Populating table " + refTbl + " with the data of location table");
	try {
		stmt = msCon.createStatement();
		
		sql = "INSERT INTO " + redShiftSchemaNamePreStage + "." + refTbl 
				+ " SELECT a.location_id , a.name as location , a.address, a.parent_id, "
				+ "b.name as parent_location,b.address as parent_loc_address FROM "+ redShiftSchemaNamePreStage + ".locations a, " 
				+ redShiftSchemaNamePreStage + ".locations b "
				+ " WHERE a.parent_id = b.location_id(+);";
		
		int res = stmt.executeUpdate(sql);
		System.out.println(res + " rows are populated.");
		
		
	} catch(SQLException e) {
		e.printStackTrace();
	}
	
	
}

	public static void main(String args[]) {

		RelationerDataLoaderForLocation j1 = new RelationerDataLoaderForLocation();
		try {
			j1.processResult();
			//j1.addEmployeeBasedOnQLavel();
			
		} catch (SQLException e) {

			e.printStackTrace();
		}
	}

}
