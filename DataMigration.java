package de.zeroco.jdbc;

import java.io.FileInputStream;
import java.io.IOException;
import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import de.zeroco.utility.Utility;

public class DataMigration {

	public static final String DATABASE_URL = "jdbc:mysql://localhost:3306/zerocode?characterEncoding=utf8";
	public static final String USER = "admin";
	public static final String USER_PASSWORD = "@Chakri007";
	public static final String REGISTER_DRIVER = "com.mysql.jdbc.Driver";

	public static void main(String[] args) {
		String tables[] = { "master_provision", "master_districts", "master_sector", "master_cell", "master_village" };
		String columns[] = { "country_id", "state_id", "district_id", "sector_id", "cell_id" };
		Connection connection = DbUtility.getConnection(DATABASE_URL, USER, USER_PASSWORD);
		migrateExcelData("/home/hari/Downloads/BRDVillageList .xlsx", connection, "zerocode",
				Arrays.asList(), "name", tables, columns);
		DbUtility.closeConnection(connection);
	}

	public static String migrateExcelData(String filePath, Connection connection, String schema, List<String> column,
			String conditionColumn, String[] tableName, String[] columns) {
		if ((Utility.isBlank(filePath) || (Utility.isBlank(schema)) || (Utility.isBlank(column)) || Utility.isBlank(conditionColumn)) || (Utility.isBlank(tableName)) || (Utility.isBlank(columns))){
			return null;
		}
		try {
			int[] id = new int[6];
			Map<String, Object> map = DbUtility.get(connection, schema, "master_country", Arrays.asList(), "name",
					Arrays.asList("Revenda"));
			id[0] = map.isEmpty() ? DbUtility.getGeneratedKey(connection, schema, "master_country",
					Arrays.asList("name", "code"), Arrays.asList("Revanda", "revanda_45")) : (int) map.get("pk_id");
			XSSFWorkbook workbook = new XSSFWorkbook(new FileInputStream(filePath));
			XSSFSheet sheet = workbook.getSheetAt(0);
			boolean isFirstRowIgnored = true;
			for (Row row : sheet) {
				if (isFirstRowIgnored) {
					isFirstRowIgnored = false;
					continue;
				}
				int j = 8;
				for (int i = 1; i < 6; i++, j -= 2) {
					map = DbUtility.get(connection, schema, tableName[i - 1], Arrays.asList(), conditionColumn,
							Arrays.asList(row.getCell(j).toString()));
					if (!map.isEmpty() && i < 5) {
						id[i] = (int) map.get("pk_id");
					}
					List<Object> list = Arrays.asList(row.getCell(j).toString(), row.getCell(j + 1).toString(),
							id[i - 1]);
					if (map.isEmpty()) {
						id[i] = DbUtility.getGeneratedKey(connection, schema, tableName[i - 1],
								Arrays.asList("code", "name", columns[i - 1]), list);
					}
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return filePath;
	}
}