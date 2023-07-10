package de.zeroco.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

public class JdbcOperations {

	public static final String REGISTER_DRIVER = "com.mysql.jdbc.Driver";
	public static final String DATABASE_URL = "jdbc:mysql://localhost:3306/zerocode?characterEncoding=utf8";
	public static final String USER = "admin";
	public static final String USER_PSSWORD = "@Chakri007";

	public static void main(String[] args) {
		Connection connect = null;
		try {
			Class.forName(REGISTER_DRIVER);
			connect = DriverManager.getConnection(DATABASE_URL, USER, USER_PSSWORD);
//			System.out.println(connect);
//			Statement statement = connect.createStatement();
//			statement.execute("insert into zerocode.employee_table values(12,\"Moksha\");");
			PreparedStatement prepareStatement = connect.prepareStatement("select * from zerocode.employee_table;");
//		    prepareStatement.setInt(1, 15);
//			prepareStatement.setString(2, "RaviBasur");
			ResultSet resultSet = prepareStatement.executeQuery();
//			System.out.println(resultSet);
			resultSet.absolute(5);
//			System.out.println(resultSet.getObject(2));
			resultSet.previous();
			resultSet.next();
//			System.out.println(resultSet.getObject(2));
//			prepareStatement.executeUpdate();
			connect.close();
		} catch (Exception e) {

			e.printStackTrace();
		}

//		System.out.println(
//				getConnection("jdbc:mysql://localhost:3306/zerocode?characterEncoding=utf8", "root", "@Chakri007"));

//		System.out.println(getExecuteStatement("jdbc:mysql://localhost:3306/zerocode?characterEncoding=utf8", "admin",
//				"@Chakri007"));
		System.out.println(executeQueryWithPreparedStatement(
				"jdbc:mysql://localhost:3306/zerocode?characterEncoding=utf8", "admin", "@Chakri007"));
	}

	public static Connection getConnection(String url, String user, String password) {
		Connection connect = null;
		try {
			Class.forName(REGISTER_DRIVER);
			connect = DriverManager.getConnection(url, user, password);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return connect;
	}

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

	public static boolean getExecuteStatement(String url, String user, String password) {
		try {
			Statement statement = getConnection(url, user, password).createStatement();
			statement.execute("insert into zerocode.employee_table values(7,\"Kiran\");");
			closeConnection(getConnection(url, user, password));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return true;
	}

	public static String executeQueryWithPreparedStatement(String url, String user, String password) {
		ResultSet set = null;
		try {
			PreparedStatement statement = getConnection(url, user, password)
					.prepareStatement("SELECT * FROM employee_table");
			set = statement.executeQuery();
			set.next();
			System.out.println(set.getObject(1) + " " + set.getObject(2));
			set.close();
			statement.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return null;
	}
}
