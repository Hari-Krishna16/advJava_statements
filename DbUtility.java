package de.zeroco.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import de.zeroco.utility.Utility;

public class DbUtility {

	public static final String DATABASE_URL = "jdbc:mysql://localhost:3306/zerocode?characterEncoding=utf8";
	public static final String USER = "admin";
	public static final String USER_PSSWORD = "@Chakri007";
	public static final String REGISTER_DRIVER = "com.mysql.jdbc.Driver";

	public static void main(String[] args) throws SQLException {
//		List<String> fields = new ArrayList<>();
//		List<String> values = new ArrayList<>();
//		fields.add("student_name");
//		values.add("Gopi");
//		Connection connection = JdbcOperations
//				.getConnection("jdbc:mysql://localhost:3306/zerocode?characterEncoding=utf8", "admin", "@Chakri007");
		System.out.println(getGeneratedKey( "zerocode", "students_table",
				Arrays.asList("student_name", "student_city"), Arrays.asList("Ranga", "America")));

//		System.out.println(getListDataFromTable("zerocode", "employee_table", Arrays.asList("employee_name", "pk_id")));
	}

	/**
	 * this method is used to establish the connection between application and
	 * database
	 * 
	 * @author hari
	 * @param url
	 * @param user
	 * @param password
	 * @return connection
	 */
	public static Connection getConnection(String url, String user, String password) {
		if ((Utility.isBlank(url) && Utility.isBlank(user)) && Utility.isBlank(password))
			return null;
		Connection connect = null;
		try {
			Class.forName(REGISTER_DRIVER);
			connect = DriverManager.getConnection(url, user, password);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return connect;
	}

	/**
	 * this method are used to close the connection between application and database
	 * 
	 * @author hari
	 * @param connection
	 * @return boolean
	 */
	public static boolean closeConnection(Connection connection) {
		try {
			if (!connection.isClosed()) {
				connection.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;
	}
    
	/**
	 * this method is used to insert the data into table and get the pk_id of values
	 * 
	 * @author hari
	 * @param connection
	 * @param schema
	 * @param tableName
	 * @param columns
	 * @param value
	 * @return 
	 * @throws SQLException
	 */
	public static int getGeneratedKey( String schema, String tableName, List<String> columns,
			List<Object> value) throws SQLException {
		if ((Utility.isBlank(schema) && Utility.isBlank(tableName)) && Utility.isBlank(columns)
				&& Utility.isBlank(value))
			return 0;
		PreparedStatement statement = getConnection(DATABASE_URL, USER, USER_PSSWORD).prepareStatement(
				QueryBuilder.getInsertQuery(schema, tableName, columns), Statement.RETURN_GENERATED_KEYS);
		for (int i = 1; i <= columns.size(); i++) {
			statement.setObject(i, value.get(i - 1));
		}
		statement.executeUpdate();
		ResultSet set = statement.getGeneratedKeys();
		int rowId = -1;
		if (set.next()) {
			rowId = set.getInt(1);
		}
		statement.close();
		set.close();
		closeConnection(getConnection(DATABASE_URL, USER, USER_PSSWORD));
		return rowId;
	}

	public static Statement getListDataFromTable(String schemaName, String tableName, List<String> columns) {
		String query = QueryBuilder.getListQuery(schemaName, tableName, columns);
		System.out.println(query);
		ResultSet set = null;
		try {
			PreparedStatement statement = getConnection(DATABASE_URL, USER, USER_PSSWORD).prepareStatement(query);
			set = statement.executeQuery();
			set.next();
			System.out.println(set.getObject(1) + " " + set.getObject(2));
			set.close();
			statement.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (set != null) {
				try {
					set.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return null;
	}

	public static PreparedStatement insertDataToTable(Connection connection, String schemaName, String tableName,
			String firstField, String secondField) throws SQLException {
		if ((Utility.isBlank(schemaName) && Utility.isBlank(tableName)) && Utility.isBlank(firstField)
				&& Utility.isBlank(secondField))
			return null;
		return connection.prepareStatement("INSERT INTO " + schemaName + "." + tableName + " (" + firstField + ", "
				+ secondField + ") VALUES (?, ?);", Statement.RETURN_GENERATED_KEYS);
	}

	public static PreparedStatement getValuesFromTable(Connection connection, String schemaName, String tableName,
			String conditionField, String value) throws SQLException {
		if ((Utility.isBlank(schemaName) && Utility.isBlank(tableName)) && Utility.isBlank(conditionField)
				&& Utility.isBlank(value))
			return null;
		String sql = "SELECT * FROM " + schemaName + "." + tableName + " WHERE " + conditionField + " = ?";
		PreparedStatement preparedStatement = connection.prepareStatement(sql);
		preparedStatement.setString(1, value);
		return preparedStatement;
	}

}