package de.zeroco.jdbc;

import java.util.Arrays;
import java.util.List;

public class QueryBuilder {

	public static final String GRAVE = "`";

	public static void main(String[] args) {
		System.out.println(getInsertQuery("zerocode", "employee", Arrays.asList("pk_id", "name", "contact")));
		System.out.println(getUpdateQuery("zerocode", "employee", Arrays.asList("name", "contact"), " id"));
		System.out.println(getDeleteQuery("Zerocode", "employee", "id", "21"));
		System.out.println(getSelectQuery("zerocode", "student", Arrays.asList("name"), "id", "21"));
		System.out.println(getData("zercode", "employee"));
	}

	public static String getInsertQuery(String schema, String tableName, List<String> fields) {
		String columnNames = "";
		String values = "";
		for (String field : fields) {
			columnNames += "," + GRAVE + field + GRAVE;
			values += ", ?";
		}
		return "INSERT INTO " + GRAVE + schema + GRAVE + "." + GRAVE + tableName + GRAVE + " ("
				+ columnNames.substring(1) + ") VALUES (" + values.substring(1) + ") ;";
	}

	public static String getUpdateQuery(String schema, String tableName, List<String> columns, String conditionColumn) {
		String columnNames = "";
		for (String field : columns) {
			columnNames += "," + GRAVE + field + GRAVE + " = ?";
		}
		return "UPDATE " + GRAVE + schema + GRAVE + "." + GRAVE + tableName + GRAVE + " SET " + columnNames.substring(1)
				+ " WHERE " + conditionColumn + " = ?" + ";";
	}

	public static String getDeleteQuery(String schema, String tableName, String condition, String value) {
		return "DELETE FROM " + GRAVE + schema + GRAVE + "." + GRAVE + tableName + GRAVE + " WHERE " + condition + " = "
				+ value + ";";
	}

	public static String getSelectQuery(String schema, String tableName, List<String> fields, String condition,
			String value) {
		String selectClause = "";
		if (fields.isEmpty()) {
			selectClause = "*";
		} else {
			for (String field : fields) {
				selectClause += "," + GRAVE + field + GRAVE;
			}
			selectClause = selectClause.substring(1);
		}
		return "SELECT " + selectClause + " FROM " + GRAVE + schema + GRAVE + "." + GRAVE + tableName + GRAVE
				+ " WHERE " + condition + " = " + value+ ";";
	}

	public static String getData(String schema, String tableName) {
		return "select * from" + GRAVE + schema + GRAVE + "." + tableName+" ;";
	}
}
