package de.zeroco.jdbc;

import java.io.File;
import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Iterator;
import java.util.regex.Pattern;

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
		
		String values = "SELECT SQL_CALC_FOUND_ROWS\n" + "    m.pk_id `uid`, \n"
				+ "    m.deleted_status `deleted_status`, \n" + "    m.facility_amount `facility_amount`, \n"
				+ "        m.outstanding_amount `outstanding_amount.zc_double`, \n"
				+ "    m.project_id `project_id`, \n" + "    m.title `title`, \n"
				+ "    m.created_time `created_time.zc_timestamp`, \n"
				+ "    mr1.pk_id `facility_amount_currency.uid`, \n"
				+ "        mr1.code `facility_amount_currency.code`, \n"
				+ "        mr1.icon `facility_amount_currency.icon`, \n"
				+ "        mr1.name `facility_amount_currency.name`, \n" + "    mr2.pk_id `sector_one.uid`, \n"
				+ "        mr2.name `sector_one.name`, \n" + "    mr3.pk_id `project_company.uid`, \n"
				+ "        mr3.is_primary `project_company.is_primary`, \n"
				+ "    mr3r4.pk_id `project_company.company.uid`, \n"
				+ "        mr3r4.name `project_company.company.name`, \n" + "    mr5.pk_id `status.uid`, \n"
				+ "        mr5.background_color `status.background_color`, \n" + "        mr5.code `status.code`, \n"
				+ "        mr5.icon `status.icon`, \n" + "        mr5.name `status.name`, \n"
				+ "        mr5.text_color `status.text_color`, \n" + "    mr6.pk_id `stage.uid`, \n"
				+ "        mr6.background_color `stage.background_color`, \n" + "        mr6.code `stage.code`, \n"
				+ "        mr6.icon `stage.icon`, \n" + "        mr6.name `stage.name`, \n"
				+ "        mr6.text_color `stage.text_color`\n" + "FROM \n" + "    `project` m         \n"
				+ "	LEFT JOIN `currency_master` mr1 ON mr1.pk_id  = m.facility_amount_currency AND  IFNULL(mr1.`is_deleted`, 0) = 0        \n"
				+ "	LEFT JOIN `sector_one` mr2 ON mr2.pk_id  = m.sector_one AND  IFNULL(mr2.`is_deleted`, 0) = 0         \n"
				+ "	JOIN `project_company` mr3 ON mr3.project = m.pk_id  AND  IFNULL(mr3.`is_deleted`, 0) = 0  AND mr3.is_primary =  ? \n"
				+ " 	JOIN `company` mr3r4 ON mr3r4.pk_id  = mr3.company AND  IFNULL(mr3r4.`is_deleted`, 0) = 0         \n"
				+ "	JOIN `project_status` mr5 ON mr5.pk_id  = m.status AND  IFNULL(mr5.`is_deleted`, 0) = 0         \n"
				+ "	JOIN `project_stage` mr6 ON mr6.pk_id  = m.stage AND  IFNULL(mr6.`is_deleted`, 0) = 0 \n"
				+ "WHERE       IFNULL(m.`is_deleted`, 0) = 0    AND IFNULL(m.deleted_status,'') IN ('0') AND  1=1  ORDER BY m.created_time DESC LIMIT ?,?";
		System.out.println(removeConditionsFromQuery(values));
		
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

	public static String removeConditionsFromQuery(String query) {
		return modifyWhereClauseInQuery(query.replaceAll(
				"AND\\s*IFNULL\\(.*\\.\\`is_deleted\\`, 0\\) = 0 | \\s*IFNULL\\(.*\\.\\`is_deleted\\`, 0\\) = 0", ""));
	}

	public static String modifyWhereClauseInQuery(String query) {
		if (Pattern.compile("WHERE\\s*(AND | BETWEEN)").matcher(query).find()) {
			query = query.replaceAll("WHERE\\s*(AND | BETWEEN)", "WHERE ");
		} else if (Pattern.compile("WHERE\\s*(ORDER BY | HAVING | ;)").matcher(query).find()) {
			query = query.replace("WHERE", "");
		}
		return query;
	}
}