import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.*;
import java.util.Properties;

public class HiveConnection {
	Connection conn;

	/**
	 * @param username
	 *            username for hive
	 * @param password
	 *            password for hive
	 */
	public HiveConnection(String username, String password, String hiveURI) {
		conn = this.getHiveConnection(username, password, hiveURI);
	}

	/**
	 * Create the connection with the hive interface
	 * 
	 * @param username
	 *            username to hive
	 * @param password
	 *            password to hive
	 * @return Connection object from connecting to the database
	 */
	public Connection getHiveConnection(String username, String password, String hiveURI) {
		try {
			Class.forName("org.apache.hive.jdbc.HiveDriver");
		} catch (ClassNotFoundException cnfe) {
			System.out.println("HiveDriver not found");
			cnfe.printStackTrace();
		}
		conn = null;
		try {
			System.out.println("Starting Hive connection...");
			conn = DriverManager.getConnection(hiveURI, username, password);
			System.out.println("Hive Connection Successful!");
		} catch (SQLException sqle) {
			System.out.println("SQLException in hive getConnection");
			sqle.printStackTrace();
		}
		return conn;
	}

	/**
	 * Generate the create table string for the based on the types and the
	 * required table name
	 * 
	 * @param columnNames
	 *            Array of column names
	 * @param columnTypes
	 *            Array of the SQL data types of the columns
	 * @param tableName
	 *            Desired name of the table
	 * @return String String built as the SQL Query for creating the table
	 */

	public String createORCString(String[] columnNames, String[] columnTypes,
			String tableName) {
		String result = "";
		int length = columnNames.length;
		try {
			result = "CREATE TABLE IF NOT EXISTS " + tableName + " (";
			for (int i = 0; i < length - 1; i++) {
				result += columnNames[i] + " ";
				// System.out.println(columnNames[i] + "\t" + columnTypes[i]);
				if (columnTypes[i].equals("NUMBER"))
					result += "INT, ";
				else if (columnTypes[i].equals("DATE"))
					result += "DATE, ";
				else
					result += "VARCHAR(255), ";
			}
			result += columnNames[length - 1] + " ";
			if (columnTypes[length - 1].equals("NUMBER"))
				result += "INT";
			else if (columnTypes[length - 1].equals("DATE"))
				result += "DATE";
			else
				result += "VARCHAR(255)";
			result += ") CLUSTERED BY ("
					+ columnNames[0]
					+ ") INTO 2 BUCKETS STORED AS ORC tblproperties ('transactional'='true')";
			// System.out.println(result);
		} catch (Exception e) {
			e.printStackTrace();
			return "";
		}
		return result;
	}

	/**
	 * Create a file in ORC format based on the hive connection given to it
	 * 
	 * @param conn
	 *            Connection object establishing connection to hive server
	 * @param columnNames
	 *            Array of column names
	 * @param columnTypes
	 *            Array of the SQL data types of the columns
	 * @param tableName
	 *            Desired name of the table
	 * @return void
	 */

	public void createORCFile(String[] columnNames, String[] columnTypes,
			String tableName) {
		String query = this
				.createORCString(columnNames, columnTypes, tableName);
		int test = this.executeUpdate(query);
		if (test < 0) {
			System.out.println("createORCFile Fail");
			return;
		}
		System.out.println("Create ORC Success");
		// ResultSet results = statement.executeQuery("SHOW TABLES");
		// while (results.next()) {
		// System.out.println(results.getString(1));
		// }
	}

	/**
	 * Loads all the data from a ResultSet into Hive
	 * 
	 * @param rs
	 *            ResultSet from which to load all the data
	 * @param columnNames
	 *            Array of column names
	 * @param columnTypes
	 *            Array of SQL data types of columns
	 * @param prefix
	 *            Prefix to the table name
	 * @param tableName
	 *            Table name
	 * @return void
	 */
	public void fullLoad(ResultSet rs, String[] columnNames,
			String[] columnTypes, String prefix, String tableName) {
		// int count = 0;
		int length = columnNames.length;
		String separator = ",";
		try {
			while (rs.next()) {
				// if (count == 20)
				// return;
				// count++;
				StringBuilder query = new StringBuilder("INSERT INTO TABLE "
						+ prefix + tableName + " VALUES (");
				for (int i = 0; i < length - 1; i++) {
					if (columnTypes[i].equals("NUMBER"))
						query.append(rs.getInt(columnNames[i]) + separator);
					else if (columnTypes[i].equals("DATE"))
						query.append("'" + rs.getDate(columnNames[i]) + "'"
								+ separator);
					else
						query.append("'" + rs.getString(columnNames[i]) + "'"
								+ separator);
				}
				if (columnTypes[length - 1].equals("NUMBER"))
					query.append(rs.getInt(columnNames[length - 1]) + separator);
				else if (columnTypes[length - 1].equals("DATE"))
					query.append("'" + rs.getDate(columnNames[length - 1])
							+ "'" + separator);
				else
					query.append("'" + rs.getString(columnNames[length - 1])
							+ "'");
				query.append(")");
				// System.out.println(query.toString());
				int insert = this.executeUpdate(query.toString());
				if (insert < 0) {

				}
			}
			// System.out.println(count);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	/**
	 * Generate and return the result set of executing the query in the Hive
	 * system
	 * 
	 * @param query
	 *            String query
	 * @return ResultSet after statement.executeQuery(query)
	 */
	public ResultSet executeQuery(String query) {
		try {
			System.out.println(query.toString());
			Statement statement = conn.createStatement();
			ResultSet rs = statement.executeQuery(query);
			return rs;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Run the query in hive and return the resultant value
	 * 
	 * @param query
	 *            Query to be executed
	 * @return Value returned from statement.executeUpdate(query)
	 */
	public int executeUpdate(String query) {
		try {
			System.out.println(query.toString());
			Statement statement = conn.createStatement();
			int result = statement.executeUpdate(query);
			return result;
		} catch (SQLException e) {
			e.printStackTrace();
			return -1;
		}
	}

	/**
	 * Get the meta data of a ResultSet
	 * 
	 * @param rs
	 *            ResultSet to get the meta data of
	 * @return 2D String Array with the column names and SQL data types of those
	 *         columns
	 */
	public String[][] getMetaData(ResultSet rs) {
		try {
			ResultSetMetaData rsmd = rs.getMetaData();

			int columnCount = rsmd.getColumnCount();
			String[][] metaData = new String[columnCount][2];
			for (int i = 1; i <= columnCount; i++) {
				String name = rsmd.getColumnName(i);
				metaData[i - 1][0] = name;
				String type = rsmd.getColumnTypeName(i);
				metaData[i - 1][1] = type;
			}
			return metaData;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Update the hive table
	 * 
	 * @param rdbmsConn
	 *            Connection object to RDBMS
	 * @param tableName
	 *            Table name
	 * @param columnNames
	 *            Array of names of columns
	 * @param columnTypes
	 *            Array of SQL data types of the columns
	 */
	public boolean updateTable(RDBMSConnection rdbmsConn, String prefix,
			String tableName, String[] columnNames, String[] columnTypes) {
		try {
			System.out.println("Starting updateTable...");
			int updateEntries = -1;
			int deleteEntries = -1;
			// Get the number of entries in the database
			ResultSet resultSet = this.executeQuery("SELECT COUNT(*) FROM "
					+ prefix + tableName);

			// Get the number of entries in hive
			ResultSet rs = rdbmsConn.executeQuery("SELECT COUNT(*) FROM "
					+ tableName);
			rs.next();
			int dbCount = rs.getInt(1);
			resultSet.next();
			int hiveCount = resultSet.getInt(1);
			System.out.println("hive count = " + hiveCount);
			System.out.println("db count = " + dbCount);
			// Get the most recent last_update_date to be able to compare to the
			// entries in the database
			resultSet = this.executeQuery("SELECT * FROM " + prefix + tableName
					+ " ORDER BY LAST_UPDATE_DATE DESC");
			resultSet.next();

			int[] index = this.getKeyIndex(columnNames,
					new String[] { "LAST_UPDATE_DATE" });
			Date date = resultSet.getDate(index[0]); // The latest update date
														// in hive so we can
														// compare it with the
														// other entries in
														// Oracle
			// if(updateEntries==deleteEntries)
			// return true;
			if (hiveCount == dbCount) {
				rs = rdbmsConn.executeQuery("SELECT COUNT(*) FROM " + tableName
						+ " WHERE LAST_UPDATE_DATE > TO_DATE('"
						+ date.toString()
						+ " 23:59:59','YYYY-MM-DD HH24:MI:SS')");
				rs.next();
				int updateCount = rs.getInt(1);
				if (updateCount == 0) {
					System.out.println("Update case 1.1");
					// Implies no new entries and no updated entries and
					// therefore no deleted entries either
					System.out.println("Update table successful");
					return true;
				} else {
					// Implies either new entries or updated entries or both...
					// If new entries, then there are deleted entries

					// Check if new entries
					ResultSet rs2 = rdbmsConn.executeQuery("SELECT * FROM "
							+ tableName + " WHERE CREATION_DATE > TO_DATE('"
							+ date.toString() + "','YYYY-MM-DD')");
					int newCount = this.insertNew(rs2, columnNames,
							columnTypes, prefix + tableName);
					if (newCount == 0) {
						System.out.println("Update case 1.2.1");
						updateEntries = this.updateEntries(rdbmsConn,
								columnNames, columnTypes, prefix, tableName,
								date);
						deleteEntries = 0;
						// Implies updates but no new entries so no deletes
						// either
					} else {
						System.out.println("Update case 1.2.2");
						updateEntries = this.updateEntries(rdbmsConn,
								columnNames, columnTypes, prefix, tableName,
								date);
						deleteEntries = deleteEntries(rdbmsConn, columnNames,
								columnTypes, prefix, tableName, newCount);
					}
				}
			} else if (hiveCount > dbCount) {
				System.out.println("Update case 2");
				ResultSet rs2 = rdbmsConn.executeQuery("SELECT * FROM "
						+ tableName + " WHERE CREATION_DATE > TO_DATE('"
						+ date.toString()
						+ " 23:59:59','YYYY-MM-DD HH24:MI:SS')");
				int newCount = insertNew(rs2, columnNames, columnTypes, prefix
						+ tableName);
				updateEntries = this.updateEntries(rdbmsConn, columnNames,
						columnTypes, prefix, tableName, date);
				deleteEntries = deleteEntries(rdbmsConn, columnNames,
						columnTypes, prefix, tableName, newCount
								+ (hiveCount - dbCount));
			} else { // More entries in database than hive
				// System.out.println("SELECT * FROM " + tableName
				// + " WHERE CREATION_DATE > TO_DATE('" + date.toString()
				// + "','YYYY-MM-DD')");
				ResultSet rs2 = rdbmsConn.executeQuery("SELECT * FROM "
						+ tableName + " WHERE CREATION_DATE > TO_DATE('"
						+ date.toString()
						+ " 23:59:59','YYYY-MM-DD HH24:MI:SS')");
				int newCount = insertNew(rs2, columnNames, columnTypes, prefix
						+ tableName);
				System.out.println("newCount = " + newCount);
				if (newCount != dbCount - hiveCount) { // Unequal number of new
														// entries and
														// difference in entries
					System.out.println("Update case 3.1");
					updateEntries = this.updateEntries(rdbmsConn, columnNames,
							columnTypes, prefix, tableName, date);
					deleteEntries = deleteEntries(rdbmsConn, columnNames,
							columnTypes, prefix, tableName, newCount
									+ (hiveCount - dbCount));
				} else { // As many new entries as
					// the increase in
					// entries
					System.out.println("Update case 3.2");
					// More new entries than the increase in the size
					updateEntries = this.updateEntries(rdbmsConn, columnNames,
							columnTypes, prefix, tableName, date);
					deleteEntries = 0;
				}
			}
			if (updateEntries < 0 || deleteEntries < 0)
				return false;
			System.out.println("Update table successful!");
			return true;
		} catch (SQLException e) {
			e.printStackTrace();
			return false;
		}
	}

	/**
	 * Checks if the hive entries exist in the database and delete the ones that
	 * don't from hive
	 * 
	 * @param rdbmsConn
	 *            RDBMSConnection object to the database
	 * @param columnNames
	 *            Array of the column names
	 * @param columnTypes
	 *            Array of the SQL data types of the columns
	 * @param prefix
	 *            prefix to the table name
	 * @param tableName
	 *            Table name
	 * @param count
	 *            Number of entries to delete
	 * @return Number of deleted entries
	 */
	private int deleteEntries(RDBMSConnection rdbmsConn, String[] columnNames,
			String[] columnTypes, String prefix, String tableName, int count) {
		try {
			System.out.println("Starting delete...");
			System.out.println("count = " + count);
			int i;
			// Get all entries from hive to see if it exists in the database
			ResultSet rs = this.executeQuery("SELECT * FROM " + prefix
					+ tableName);
			String separator = " AND ";
			while (rs.next()) {
				if (count == 0) {
					System.out.println("Delete successful");
					return 0;
				}
				Properties prop = new Properties();
				InputStream is = new FileInputStream("config.properties");
				prop.load(is);
				// Property that stores the list of columns that allow unique
				// search results
				String[] keys = prop.getProperty(prefix + tableName).split(",");
				String[] keyTypes = this.getKeyTypes(columnNames, columnTypes,
						keys);
				int[] indices = this.getKeyIndex(columnNames, keys);
				// this.printArray(keys);
				// this.printArray(keyTypes);
				int l = keys.length, length = l;
				StringBuilder query = new StringBuilder("SELECT * FROM "
						+ tableName + " WHERE ");
				for (i = 0; i < l - 1; i++) {
					if (keyTypes[i].equals("NUMBER"))
						query.append(keys[i] + "=" + rs.getInt(indices[i])
								+ separator);
					else if (keyTypes[i].equals("DATE"))
						query.append(keys[i] + ">=TO_DATE('"
								+ rs.getDate(indices[i])
								+ " 00:00:00','YYYY-MM-DD HH24:MI:SS')"
								+ separator + keys[i] + "<=TO_DATE('"
								+ rs.getDate(indices[i])
								+ " 23:59:59','YYYY-MM-DD HH24:MI:SS')"
								+ separator);
					else
						query.append(keys[i] + "='" + rs.getString(indices[i])
								+ "'" + separator);
				}
				if (keyTypes[length - 1].equals("NUMBER")) {
					System.out.println(keyTypes[length - 1]);
					query.append(keys[length - 1] + "="
							+ rs.getInt(indices[length - 1]));
				} else if (keyTypes[length - 1].equals("DATE"))
					query.append(keys[length - 1] + ">=TO_DATE('"
							+ rs.getDate(indices[length - 1])
							+ " 00:00:00','YYYY-MM-DD HH24:MI:SS')" + separator
							+ keys[i] + "<=TO_DATE('" + rs.getDate(indices[i])
							+ " 23:59:59','YYYY-MM-DD HH24:MI:SS')");
				else
					query.append(keys[length - 1] + "='"
							+ rs.getString(indices[length - 1]) + "'");

				// Check if the row still exists in the database
				ResultSet rdbmsrs = rdbmsConn.executeQuery(query.toString());
				if (!rdbmsrs.next()) {
					StringBuilder query2 = new StringBuilder("DELETE FROM "
							+ prefix + tableName + " WHERE ");
					for (i = 0; i < l - 1; i++) {
						if (keyTypes[i].equals("NUMBER"))
							query2.append(keys[i] + "=" + rs.getInt(indices[i])
									+ separator);
						else if (keyTypes[i].equals("DATE"))
							query2.append(keys[i] + "='"
									+ rs.getDate(indices[i]) + "'" + separator);
						else
							query2.append(keys[i] + "='"
									+ rs.getString(indices[i]) + "'"
									+ separator);
					}
					if (keyTypes[length - 1].equals("NUMBER"))
						query2.append(keys[length - 1] + "="
								+ rs.getInt(indices[length - 1]));
					else if (keyTypes[length - 1].equals("DATE"))
						query2.append(keys[length - 1] + "='"
								+ rs.getDate(indices[length - 1]) + "'");
					else
						query2.append(keys[length - 1] + "='"
								+ rs.getString(indices[length - 1]) + "'");
					int delete = this.executeUpdate(query2.toString());
					if (delete >= 0)
						count--;
					else {
						System.out.println("Delete error -1");
						return -1;
					}

				}

			}
		} catch (Exception e) {
			e.printStackTrace();
			return count;
		}

		return 0;
	}

	/**
	 * Insert new entries into hive and return the count of the new entries
	 * 
	 * @param rs2
	 *            ResultSet object of new entries in the database
	 * @param columnNames
	 *            Array of the column names
	 * @param columnTypes
	 *            Array of the SQL data types of the columns
	 * @param tableName
	 *            Table name
	 * @return int count of the number of new entries inserted
	 */
	private int insertNew(ResultSet rs2, String[] columnNames,
			String[] columnTypes, String tableName) {
		int count = 0;
		try {
			System.out.println("Starting insertNew...");
			int length = columnNames.length;
			String separator = ",";
			while (rs2.next()) {
				// if (count >= 10)
				// return 10;
				StringBuilder query = new StringBuilder("INSERT INTO TABLE "
						+ tableName + " VALUES (");
				for (int i = 0; i < length - 1; i++) {
					if (columnTypes[i].equals("NUMBER"))
						query.append(rs2.getInt(columnNames[i]) + separator);
					else if (columnTypes[i].equals("DATE"))
						query.append("'" + rs2.getDate(columnNames[i]) + "'"
								+ separator);
					else
						query.append("'" + rs2.getString(columnNames[i]) + "'"
								+ separator);
				}
				if (columnTypes[length - 1].equals("NUMBER"))
					query.append(rs2.getInt(columnNames[length - 1]));
				else if (columnTypes[length - 1].equals("DATE"))
					query.append("'" + rs2.getDate(columnNames[length - 1]));
				else
					query.append("'" + rs2.getString(columnNames[length - 1])
							+ "'");
				query.append(")");
				// System.out.println(query.toString());
				int insert = this.executeUpdate(query.toString());
				if (insert < 0)
					System.out.println("Insert new failed");
				else
					count++;
			}
			System.out.println("InsertNew Successful");
			return count;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		System.out.println("InsertNew Successful");
		return count;
	}

	/**
	 * Get the most recent date in the table
	 * 
	 * @param columnNames
	 *            Array of column names of the table
	 * @param tableName
	 *            Table name
	 * @return date Most recent date in the table
	 */
	public Date getLastDate(String[] columnNames, String tableName) {
		try {
			ResultSet rs = this.executeQuery("SELECT * FROM " + tableName
					+ " ORDER BY LAST_UPDATE_DATE DESC");
			rs.next();
			int[] index = this.getKeyIndex(columnNames,
					new String[] { "LAST_UPDATE_DATE" });
			Date date = rs.getDate(index[0]);
			return date;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Update the entries in hive which have been updated in the database
	 * 
	 * @param conn
	 *            Connection object connecting to the database
	 * @param columnNames
	 *            Array of column names
	 * @param columnTypes
	 *            Array of SQL data types of the columns
	 * @param prefix
	 *            prefix to the table name
	 * @param tableName
	 *            Name of the table
	 * @return boolean true if entries are successfully updated. False otherwise
	 */
	public int updateEntries(RDBMSConnection rdbmsConn, String[] columnNames,
			String[] columnTypes, String prefix, String tableName, Date date) {
		try {
			System.out.println("Starting updateEntries...");
			Properties prop = new Properties();
			InputStream is = new FileInputStream("config.properties");
			prop.load(is);
			int count = 0;
			int length = columnNames.length;
			String separator = ",";
			String[] keys = prop.getProperty(prefix + tableName).split(",");
			ResultSet resultSet = rdbmsConn
					.executeQuery("SELECT * FROM "
							+ tableName
							+ " WHERE LAST_UPDATE_DATE > TO_DATE('"
							+ date
							+ " 23:59:59','YYYY-MM-DD HH24:MI:SS') AND CREATION_DATE <= TO_DATE('"
							+ date + " 23:59:59', 'YYYY-MM-DD HH24:MI:SS')");
			while (resultSet.next()) {
				StringBuilder query = new StringBuilder("UPDATE " + prefix
						+ tableName + " SET ");
				for (int i = 0; i < length - 1; i++) {
					if (columnTypes[i].equals("NUMBER"))
						query.append(columnNames[i] + "="
								+ resultSet.getInt(columnNames[i]) + separator);
					else if (columnTypes[i].equals("DATE"))
						query.append(columnNames[i] + "='"
								+ resultSet.getDate(columnNames[i]) + "'"
								+ separator);
					else
						query.append(columnNames[i] + "='"
								+ resultSet.getString(columnNames[i]) + "'"
								+ separator);
				}
				if (columnTypes[length - 1].equals("NUMBER"))
					query.append(columnNames[length - 1] + "="
							+ resultSet.getInt(columnNames[length - 1])
							+ separator);
				else if (columnTypes[length - 1].equals("DATE"))
					query.append(columnNames[length - 1] + "='"
							+ resultSet.getDate(columnNames[length - 1]) + "'"
							+ separator);
				else
					query.append(columnNames[length - 1] + "='"
							+ resultSet.getString(columnNames[length - 1])
							+ "'");
				query.append(" WHERE ");
				int len = keys.length;
				String[] keyTypes = getKeyTypes(columnNames, columnTypes, keys);
				int[] indices = this.getKeyIndex(columnNames, keys);
				for (int i = 0; i < len; i++) {
					if (keyTypes[i].equals("NUMBER"))
						query.append(keys[i] + "="
								+ resultSet.getInt(indices[i]) + "AND");
					else if (keyTypes[i].equals("DATE"))
						query.append(keys[i] + "='"
								+ resultSet.getDate(indices[i]) + "'" + "AND");
					else
						query.append(keys[i] + "='"
								+ resultSet.getString(indices[i]) + "'" + "AND");
				}
				if (keyTypes[length - 1].equals("NUMBER"))
					query.append(keys[length - 1] + "="
							+ resultSet.getInt(indices[length - 1]));
				else if (keyTypes[length - 1].equals("DATE"))
					query.append(indices[length - 1] + "='"
							+ resultSet.getDate(indices[length - 1]) + "'");
				else
					query.append(keys[length - 1] + "='"
							+ resultSet.getString(indices[length - 1]) + "'");
				// System.out.println(query.toString());
				int insert = this.executeUpdate(query.toString());
				if (insert < 0) {
					System.out.println("UpdateEntries error -1");
					return -1;
				}
				count++;
			}
			System.out.println("UpdateEntries successful");
			return count;
		} catch (Exception e) {
			e.printStackTrace();
			return -1;
		}
	}

	/**
	 * Get the SQL Data Types of the keys
	 * 
	 * @param columnNames
	 *            Array of Column Names of the tables
	 * @param columnTypes
	 *            Array of SQL data types of the columns
	 * @param keys
	 *            Array of the keys
	 * @return Array of the SQL data types of the keys
	 */
	private String[] getKeyTypes(String[] columnNames, String[] columnTypes,
			String[] keys) {
		int i, j, l = keys.length, len = columnNames.length, count = 0;
		String[] keyTypes = new String[l];
		for (i = 0; i < l; i++) {
			for (j = 0; j < len; j++) {
				if (columnNames[j].equals(keys[i])) {
					keyTypes[count++] = columnTypes[j];
				}
			}
		}
		// this.printArray(keys);
		// this.printArray(keyTypes);
		return keyTypes;
	}

	/**
	 * Return the index of the columns of the keys
	 * 
	 * @param columnNames
	 *            Array of all columns
	 * @param keys
	 *            Array of the keys
	 * @return Array with the indices of the keys
	 */
	private int[] getKeyIndex(String[] columnNames, String[] keys) {
		int i, j, count = 0, l = keys.length, len = columnNames.length;
		int[] index = new int[l];
		for (i = 0; i < l; i++) {
			for (j = 0; j < len; j++) {
				if (keys[i].equals(columnNames[j]))
					index[count++] = j + 1;
			}
		}
		return index;
	}

	/**
	 * Print out the contents of an array
	 * 
	 * @param arr
	 *            Array to be printed
	 */
	public void printArray(Object[] arr) {
		int l = arr.length;
		System.out.println();
		for (int i = 0; i < l; i++) {
			System.out.println(arr[i].toString());
		}
		System.out.println();
	}

}
