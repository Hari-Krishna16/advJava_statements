package de.zeroco.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;

public class DbUtility {

	public static final String REGISTER_DRIVER = "com.mysql.jdbc.Driver";

	public static void main(String[] args) {

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

}
