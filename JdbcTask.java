package de.zeroco.jdbc;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import de.zeroco.strings.StringTasks;

public class JdbcTask {

	public static void main(String[] args) throws SQLException {
		String filePath = "/home/hari/Downloads/Districts.xlsx";
		Connection connection = JdbcOperations
				.getConnection("jdbc:mysql://localhost:3306/zerocode?characterEncoding=utf8", "admin", "@Chakri007");
		migrateExcelDataToDatabases(filePath, 0, 1, connection);
	}

	public static Object migrateExcelDataToDatabase(String filePath, int stateColumnIndex, int districtColumnIndex,
			Connection connection) {
		String state = "";
		String district = "";
		Map<String, String> statesMap = new HashMap<>();
		Map<String, List<String>> districtsMap = new HashMap<>();
		Cell stateCell, districtCell;
		try {
			XSSFWorkbook workbook = new XSSFWorkbook(new FileInputStream(new File(filePath)));
			XSSFSheet sheet = workbook.getSheetAt(0);
			Iterator<Row> rowIterator = sheet.iterator();
			if (rowIterator.hasNext()) {
				rowIterator.next();
			}
			while (rowIterator.hasNext()) {
				Row row = rowIterator.next();
				stateCell = row.getCell(stateColumnIndex);
				districtCell = row.getCell(districtColumnIndex);
				if (stateCell != null && districtCell != null) {
					state = stateCell.getStringCellValue();
					district = districtCell.getStringCellValue();
					if (!statesMap.containsKey(state)) {
						statesMap.put(state, state);
						districtsMap.put(state, new ArrayList<>());
					}
					districtsMap.get(state).add(district);
				}
			}
			String stateQuery = "INSERT INTO state_table (state_name, state_code) VALUES (?, ?)";
			String districtQuery = "INSERT INTO district_table (district_name, state_id) VALUES (?, ?)";
			PreparedStatement stateStatement = connection.prepareStatement(stateQuery, Statement.RETURN_GENERATED_KEYS);
			PreparedStatement districtStatement = connection.prepareStatement(districtQuery);
			for (Map.Entry<String, String> entry : statesMap.entrySet()) {
				String stateData = entry.getValue();
				String formattedData = StringTasks.getFormattedValue(stateData);
				stateStatement.setString(1, stateData);
				stateStatement.setString(2, formattedData);
				stateStatement.executeUpdate();
				ResultSet generatedKeys = stateStatement.getGeneratedKeys();
				int stateId = -1;
				if (generatedKeys.next()) {
					stateId = generatedKeys.getInt(1);
				}
				List<String> districts = districtsMap.get(stateData);
				for (int i = 0; i < districts.size(); i++) {
					String districtData = districts.get(i);
					districtStatement.setString(1, districtData);
					districtStatement.setInt(2, stateId);
					districtStatement.executeUpdate();
				}
			}
			connection.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "Data Migreted Sucessfully";
	}
	
	public static Object migrateExcelDataToDatabases(String filePath, int stateColumnIndex, int districtColumnIndex,
	        Connection connection) {
	    String state = "";
	    String district = "";
	    Cell stateCell, districtCell;
	    try {
	        XSSFWorkbook workbook = new XSSFWorkbook(new FileInputStream(new File(filePath)));
	        XSSFSheet sheet = workbook.getSheetAt(0);
	        Iterator<Row> rowIterator = sheet.iterator();
	        if (rowIterator.hasNext()) {
	            rowIterator.next();
	        }
	        while (rowIterator.hasNext()) {
	            Row row = rowIterator.next();
	            stateCell = row.getCell(stateColumnIndex);
	            districtCell = row.getCell(districtColumnIndex);
	            if (stateCell != null && districtCell != null) {
	                state = stateCell.getStringCellValue();
	                district = districtCell.getStringCellValue();
	                if (!stateExists(connection, state)) {
	                    PreparedStatement stateStatement = insertDataToTable(connection, "state_table", "state_name", "state_code");
	                    stateStatement.setString(1, state);
	                    stateStatement.setString(2, StringTasks.getFormattedValue(state));
	                    stateStatement.executeUpdate();
	                }
	                int stateId = getStateId(connection, state);
	                PreparedStatement districtStatement = insertDataToTable(connection, "district_table", "district_name", "state_id");
	                districtStatement.setString(1, district);
	                districtStatement.setInt(2, stateId);
	                districtStatement.executeUpdate();
	            }
	        }
	        connection.close();
	    } catch (Exception e) {
	        e.printStackTrace();
	    }
	    return "Data Migrated Successfully";
	}
	public static PreparedStatement insertDataToTable(Connection connection, String tableName, String firstField, String secondField)
	        throws SQLException {
	    String sql = "INSERT INTO " + tableName + " (" + firstField + ", " + secondField + ") VALUES (?, ?)";
	    PreparedStatement preparedStatement = connection.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
	    return preparedStatement;
	}

	public static PreparedStatement getValuesFromTable(Connection connection, String tableName, String conditionField, String value)
	        throws SQLException {
	    String sql = "SELECT * FROM " + tableName + " WHERE " + conditionField + " = ?";
	    PreparedStatement preparedStatement = connection.prepareStatement(sql);
	    preparedStatement.setString(1, value);
	    return preparedStatement;
	}

	public static boolean stateExists(Connection connection, String state) throws SQLException {
	    PreparedStatement statement = getValuesFromTable(connection, "state_table", "state_name", state);
	    ResultSet resultSet = statement.executeQuery();
	    return resultSet.next();
	}

	public static int getStateId(Connection connection, String state) throws SQLException {
	    PreparedStatement statement = getValuesFromTable(connection, "state_table", "state_name", state);
	    ResultSet resultSet = statement.executeQuery();
	    if (resultSet.next()) {
	        return resultSet.getInt(1);
	    }
	    return -1;
	}


}