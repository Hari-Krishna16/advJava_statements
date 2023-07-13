package de.zeroco.jdbc;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;

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
		System.out.println(migrateExcelDataToDatabases(filePath, 0, 1, connection));
	}
	
	public static Object migrateExcelDataToDatabases(String filePath, int stateColumnIndex, int districtColumnIndex,
			Connection connection) {
		String state = "";
		String district = "";
		int rowId = 0;
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
					if (!isValueExists(connection, "zerocode", "state_table", "state_name", state)) {
						PreparedStatement stateStatement = DbUtility.insertDataToTable(connection, "zerocode",
								"state_table", "state_name", "state_code");
						stateStatement.setString(1, state);
						stateStatement.setString(2, StringTasks.getFormattedValue(state));
						stateStatement.executeUpdate();
						ResultSet set = stateStatement.getGeneratedKeys();
						if (set.next()) {
							rowId = set.getInt(1);
							System.out.println(rowId);
						}
					}
					if (!isValueExists(connection, "zerocode", "district_table", "district_name", district)) {
						PreparedStatement districtStatement = DbUtility.insertDataToTable(connection, "zerocode",
								"district_table", "district_name", "state_id");
						districtStatement.setString(1, district);
						districtStatement.setInt(2, rowId);
						districtStatement.executeUpdate();
					}
				}
			}
			DbUtility.closeConnection(connection);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "Data Migration Sucessfull ";
	}

	public static boolean isValueExists(Connection connection, String schemaName, String tableName, String columnName,
			String state) throws SQLException {
		PreparedStatement statement = DbUtility.getValuesFromTable(connection, schemaName, tableName, columnName,
				state);
		ResultSet resultSet = statement.executeQuery();
		return resultSet.next();
	}

}