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
		connection.close();
		System.out.println(migrateExcelDataToDatabase(filePath, 1, 0, connection));
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
		} catch (Exception e) {
			e.printStackTrace();
		}
		return "Data Migreted Sucessfully";
	}
}