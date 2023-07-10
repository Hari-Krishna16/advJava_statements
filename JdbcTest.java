package de.zeroco.jdbc;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

public class JdbcTest {

	public static void main(String[] args) {
		// Assuming you have established the database connection earlier
		String filePath = "/home/hari/Downloads/Districts.xlsx";
		int stateColumnIndex = 0; // Index of the column containing the state values
		Connection connection = establishConnection();
		addStatesToDatabase(filePath, stateColumnIndex, connection);
		closeConnection(connection);
	}

	public static Connection establishConnection() {
		Connection connection = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			String url = "jdbc:mysql://localhost:3306/zerocode?characterEncoding=utf8";
			String username = "admin";
			String password = "@Chakri007";
			connection = DriverManager.getConnection(url, username, password);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return connection;
	}

	public static void closeConnection(Connection connection) {
		try {
			if (connection != null) {
				connection.close();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static void addStatesToDatabase(String filePath, int stateColumnIndex, Connection connection) {
	    Map<Integer, String> statesMap = new HashMap<>();
	    Cell cell = null;
	    Set<String> uniqueStates = new HashSet<>();
	    try {
	        XSSFWorkbook workbook = new XSSFWorkbook(new FileInputStream(new File(filePath)));
	        XSSFSheet sheet = workbook.getSheetAt(0);
	        Iterator<Row> rowIterator = sheet.iterator();
	        int rowIndex = 1;
	        // Skip the first row (assuming it contains column names)
	        if (rowIterator.hasNext()) {
	            rowIterator.next();
	        }
	        while (rowIterator.hasNext()) {
	            Row row = rowIterator.next();
	            cell = row.getCell(stateColumnIndex);
	            if (cell != null) {
	                String state = cell.getStringCellValue();
	                if (!uniqueStates.contains(state)) {
	                    statesMap.put(rowIndex, state);
	                    uniqueStates.add(state);
	                    rowIndex++;
	                }
	            }
	        }
			String sql = "INSERT INTO state_table (pk_id,state_name,state_code) VALUES (?, ?,?)";
			PreparedStatement statement = connection.prepareStatement(sql);
			for (Map.Entry<Integer, String> entry : statesMap.entrySet()) {
				int id = entry.getKey();
				String data = entry.getValue();
				String formattedValue = JdbcPractice.getFormattedValue(data);
				System.out.println(formattedValue);
				statement.setInt(1, id);
				statement.setString(2, data);
				statement.setString(3, formattedValue);
				statement.executeUpdate();
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
