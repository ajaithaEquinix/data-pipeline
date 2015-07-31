import java.sql.*;

public class RDBMSConnection {
	Connection conn;

	/**
	 * @param username
	 *            username for RDBMS
	 * @param password
	 *            password for RDBMS
	 */
	public RDBMSConnection(String username, String password, String rdbmsURI) {
		conn = this.getRDBMSConnection(username, password, rdbmsURI);
	}

	/**
	 * Create the connection with the oracle database
	 * 
	 * @param username
	 *            username for the database
	 * @param password
	 *            password for the database
	 * @return Connection object from connecting to the database
	 */

	public Connection getRDBMSConnection(String username, String password, String rdbmsURI) {
		try {
			Class.forName("oracle.jdbc.driver.OracleDriver");
		} catch (ClassNotFoundException e) {
			System.out.println("Class Not Found");
			e.printStackTrace();
		}
		conn = null;
		try {
			conn = DriverManager.getConnection(rdbmsURI, username, password);
		} catch (SQLException e) {
			System.out.println("SQLException");
			e.printStackTrace();
		}
		return conn;
	}

	/**
	 * Generate and return the result set of executing the query in the RDBMS
	 * system
	 * 
	 * @param query
	 *            String query
	 * @return ResultSet after running statement.executeQuery(query)
	 */
	public ResultSet executeQuery(String query) {
		Statement statement;
		try {
			System.out.println(query);
			statement = conn.createStatement();
			ResultSet rs = statement.executeQuery(query);
			return rs;
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}

	/**
	 * Run the query in RDBMS and return the resultant value
	 * 
	 * @param query
	 *            Query to be executed
	 * @return Value returned from statement.executeUpdate(query)
	 */
	public int executeUpdate(String query) {
		try {
			System.out.println(query);
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
}
