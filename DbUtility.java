package de.zeroco.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import de.zeroco.utility.Utility;

public class DbUtility {

	public static final String DATABASE_URL = "jdbc:mysql://localhost:3306/zerocode?characterEncoding=utf8";
	public static final String USER = "admin";
	public static final String USER_PASSWORD = "@Chakri007";
	public static final String REGISTER_DRIVER = "com.mysql.jdbc.Driver";

	public static void main(String[] args) throws SQLException {
//		List<String> fields = new ArrayList<>();
//		List<String> values = new ArrayList<>();
//		fields.add("student_name");
//		values.add("Gopi");
//		Connection connection = JdbcOperations
//				.getConnection("jdbc:mysql://localhost:3306/zerocode?characterEncoding=utf8", "admin", "@Chakri007");
//		System.out.println(getGeneratedKey( "zerocode", "students_table",
//				Arrays.asList("student_name", "student_city"), Arrays.asList("Ranga", "America")));

//		System.out.println(list("zerocode", "employee_table", Arrays.asList()));
//		System.out.println(get("zerocode", "employee_table", Arrays.asList("employee_name"),"pk_id", Arrays.asList(1)));
		System.out.println(update("zerocode", "employee_table", Arrays.asList("employee_name"),
				Arrays.asList("Bahubali"), "pk_id", 6));
		System.out.println(delete("zerocode", "employee_table", "pk_id", 13));
	}

	/**
	 * this method is used to establish the connection between application and
	 * database
	 * 
	 * @author HariKrishna kaki
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
	 * @author HariKrishna kaki
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
	 * @author HariKrishna kaki
	 * @param connection
	 * @param schema
	 * @param tableName
	 * @param columns
	 * @param value
	 * @return
	 * @throws SQLException
	 */
	public static int getGeneratedKey(String schema, String tableName, List<String> columns, List<Object> value) {
		if ((Utility.isBlank(schema) && Utility.isBlank(tableName)) && Utility.isBlank(columns)
				&& Utility.isBlank(value))
			return 0;
		PreparedStatement statement;
		int rowId = 0;
		try {
			statement = getConnection(DATABASE_URL, USER, USER_PASSWORD).prepareStatement(
					QueryBuilder.getInsertQuery(schema, tableName, columns), Statement.RETURN_GENERATED_KEYS);
			for (int i = 1; i <= columns.size(); i++) {
				statement.setObject(i, value.get(i - 1));
			}
			statement.executeUpdate();
			ResultSet set = statement.getGeneratedKeys();
			if (set.next()) {
				rowId = set.getInt(1);
			}
			statement.close();
			set.close();
			closeConnection(getConnection(DATABASE_URL, USER, USER_PASSWORD));
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return rowId;
	}

	/**
	 * this method is used to get the Table in listOf maps
	 * 
	 * @author HariKrishna kaki
	 * @param schema
	 * @param tableName
	 * @param columns
	 * @return list of maps
	 */
	public static List<Map<String, Object>> list(String schema, String tableName, List<String> columns) {
		if ((Utility.isBlank(schema) && Utility.isBlank(tableName)) && Utility.isBlank(columns))
			return null;
		List<Map<String, Object>> listOfMaps = new ArrayList<>();
		String columnName = "";
		Object columnValue = "";
		int countColumns = 0;
		Connection connection = null;
		try {
			connection = DbUtility.getConnection(DATABASE_URL, USER, USER_PASSWORD);
			PreparedStatement statement = connection
					.prepareStatement(QueryBuilder.getListQuery(schema, tableName, columns));
			ResultSet set = statement.executeQuery();
			ResultSetMetaData metaData = set.getMetaData();
			countColumns = metaData.getColumnCount();
			while (set.next()) {
				Map<String, Object> map = new HashMap<>();
				for (int i = 1; i <= countColumns; i++) {
					columnName = metaData.getColumnName(i);
					columnValue = set.getObject(i);
					map.put(columnName, columnValue);
				}
				listOfMaps.add(map);
			}
			statement.close();
			set.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbUtility.closeConnection(connection);
		}

		return listOfMaps;
	}

	/**
	 * this method is used to get the data from the table in database
	 * 
	 * @author HariKrishna kaki
	 * @param schema
	 * @param tableName
	 * @param columns
	 * @param conditionColumn
	 * @param values
	 * @return map
	 */
	public static Map<String, Object> get(String schema, String tableName, List<String> columns, String conditionColumn,
			List<Object> values) {
		if ((Utility.isBlank(schema) && Utility.isBlank(tableName)) && (Utility.isBlank(columns))
				&& (Utility.isBlank(conditionColumn)) && (Utility.isBlank(values)))
			return null;
		String columnName = "";
		Object columnValue = "";
		int countColumns = 0;
		Connection connection = null;
		Map<String, Object> map = new HashMap<>();
		System.out.println(QueryBuilder.getQuerys(schema, tableName, columns, conditionColumn, values));
		try {
			connection = DbUtility.getConnection(DATABASE_URL, USER, USER_PASSWORD);
			PreparedStatement statement = connection
					.prepareStatement(QueryBuilder.getQuerys(schema, tableName, columns, conditionColumn, values));
			ResultSet set = statement.executeQuery();
			ResultSetMetaData metaData = set.getMetaData();
			countColumns = metaData.getColumnCount();
			while (set.next()) {
				for (int i = 1; i <= countColumns; i++) {
					columnName = metaData.getColumnName(i);
					columnValue = set.getObject(i);
					if (!map.containsKey(columnName)) {
						map.put(columnName, columnValue);
					}
				}
			}
			statement.close();
			set.close();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbUtility.closeConnection(connection);
		}
		return map;
	}

	/**
	 * this method is used to update values of a table in database
	 * 
	 * @author HariKrishna kaki
	 * @param schema
	 * @param tableName
	 * @param columns
	 * @param values
	 * @param conditionColumn
	 * @param value
	 * @return number of rows updated
	 */
	public static int update(Connection connection, String schema, String tableName, List<String> columns, List<Object> values,
			String conditionColumn, Object value) {
		if ((Utility.isBlank(schema) && Utility.isBlank(tableName)) && (Utility.isBlank(columns))
				&& (Utility.isBlank(conditionColumn)) && (Utility.isBlank(values)) && (Utility.isBlank(value)))
			return 0;
		connection = getConnection(DATABASE_URL, USER, USER_PASSWORD);
		int effectedRows = 0;
		try {
			String query = QueryBuilder.getUpdateQuery(schema, tableName, columns, conditionColumn, value);
			PreparedStatement statement = connection.prepareStatement(query);
			int i = 1;
				for (Object key : values) {
					statement.setObject(i , key);
					i++;
				}
			effectedRows = statement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			closeConnection(connection);
		}
		return effectedRows;
	}

	/**
	 * this method is used to delete row data from a table in database
	 * 
	 * @author HariKrishna kaki
	 * @param schema
	 * @param tableName
	 * @param conditionColumn
	 * @param value
	 * @return
	 */
	public static int delete(String schema, String tableName, String conditionColumn, Object value) {
		if ((Utility.isBlank(schema) && Utility.isBlank(tableName))
				&& (Utility.isBlank(conditionColumn) && (Utility.isBlank(value))))
			return 0;
		Connection conection = getConnection(DATABASE_URL, USER, USER_PASSWORD);
		int rowsDeleted = 0;
		try {
			PreparedStatement statement = conection
					.prepareStatement(QueryBuilder.getDeleteQuery(schema, tableName, conditionColumn));
			statement.setObject(1, value);
			rowsDeleted = statement.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return rowsDeleted;
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
