import java.io.*;
import java.sql.*;
import java.util.Properties;

public class pipeline {
	/**
	 * Runs the program. Pull the data from the oracle database and store it
	 * locally using helper functions and also checks for updates to the schema
	 * and the data.
	 * 
	 * @param args
	 * @return void
	 */

	public static void main(String[] args) {
		try {
			Properties prop = new Properties();
			InputStream is = new FileInputStream("config.properties");
			prop.load(is);
			String rdbmsUsername = prop.getProperty("rdbmsUsername");
//			System.out.println("rdbmsun = " + rdbmsUsername);
			String rdbmsPassword = prop.getProperty("rdbmsPassword");
//			System.out.println("rdbmspw = " + rdbmsPassword);
			String hiveUsername = prop.getProperty("hiveUsername");
//			System.out.println("hiveun = " + hiveUsername);
			String hivePassword = prop.getProperty("hivePassword");
//			System.out.println("hivepw = " + hivePassword);
			boolean check = true;
			String rdbmsURI = prop.getProperty("rdbmsURI");
			String hiveURI = prop.getProperty("hiveURI");
			RDBMSConnection rdbmsConn = new RDBMSConnection(rdbmsUsername,
					rdbmsPassword, rdbmsURI);
			HiveConnection hiveConn = new HiveConnection(hiveUsername,
					hivePassword, hiveURI);

			if (rdbmsConn.conn == null) {
				System.out.println("RDBMS Null");
				check = false;
			} else {
				System.out.println("RDBMS connection successful");
			}

			if (hiveConn.conn == null) {
				System.out.println("Hive Null");
				check = false;
			} else {
				System.out.println("Hive connection successful");
			}

			if (check == false)
				return;

//			if (check == true)
//			return;
			// Get Tables
			String[] tables = prop.getProperty("tables").split(",");
			int i, j, l = tables.length;
			String prefix = prop.getProperty("prefix");

			// For each table
			for (i = 0; i < l; i++) {
				// Set current table
				String tableName = tables[i];
				ResultSet rs = rdbmsConn.executeQuery("SELECT * FROM "
						+ tableName);
				// Get meta data
				String[][] temp = rdbmsConn.getMetaData(rs);
				int length = temp.length;
				// System.out.println("length = " + length);
				String[] columnNames = new String[length];
				String[] columnTypes = new String[length];

				// Store metadata in arrays
				for (j = 0; j < length; j++) {
					columnNames[j] = temp[j][0];
					columnTypes[j] = temp[j][1];
				}

				hiveConn.createORCFile(columnNames, columnTypes, prefix
						+ tableName);
				// If previously existed, the hive table must have had entries
				ResultSet testExist = hiveConn.executeQuery("SELECT * FROM "
						+ prefix + tableName);
				if (testExist.next()) {
					// Previously existed
					// Check for all updates in the rows
					hiveConn.updateTable(rdbmsConn, prefix, tableName,
							columnNames, columnTypes);
					// TODO: Update structure of data ------- MIGHT WANT TO DO
					// THIS BEFORE THE updateTable()
				} else {
					// Just created the table or the table is empty so run full load
					rs = rdbmsConn.executeQuery("SELECT * FROM " + tableName);
					hiveConn.fullLoad(rs, columnNames, columnTypes, prefix,
							tableName);
				}
				rdbmsConn.conn.close();
				hiveConn.conn.close();
			}
		} catch (Exception sqle) {
			System.out.println("Exception main");
			sqle.printStackTrace();
		}
	}

}
