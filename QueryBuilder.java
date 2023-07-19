package de.zeroco.jdbc;

import java.util.Arrays;
import java.util.List;

import de.zeroco.utility.Utility;

public class QueryBuilder {

	public static final String GRAVE = "`";

	public static void main(String[] args) {
//		System.out.println(getInsertQuery("zerocode", "employee", Arrays.asList("pk_id", "name", "contact")));
		System.out.println(getUpdateQuery("zerocode", "employee", Arrays.asList("FirstName", "LastName"), " id","Hari"));
//		System.out.println(getDeleteQuery("Zerocode", "employee","id"));
//		System.out.println(getQuerys("zerocode", "student", Arrays.asList(), "pk_id", Arrays.asList("hari","shiva","mohan")));
//		System.out.println(getListQuery("zerocode", "district_table", Arrays.asList("employee", "roles")));

	}

	/**
	 * this method is used to generate query for insert operations
	 * 
	 * @author hari
	 * @param schema
	 * @param tableName
	 * @param columns
	 * @return Insert Query
	 */
	public static String getInsertQuery(String schema, String tableName, List<String> columns) {
		if ((Utility.isBlank(schema) && Utility.isBlank(tableName)) && Utility.isBlank(columns))
			return null;
		String columnNames = "";
		String values = "";
		for (String column : columns) {
			columnNames += "," + GRAVE + column + GRAVE;
			values += ", ?";
		}
		return "INSERT INTO " + GRAVE + schema + GRAVE + "." + GRAVE + tableName + GRAVE + " ("
				+ columnNames.substring(1) + ") VALUES (" + values.substring(1) + ") ;";
	}

	/**
	 * this method is used to generate query for update operations
	 * 
	 * @author Hari Krishna
	 * @param schema
	 * @param tableName
	 * @param columns
	 * @param conditionColumn
	 * @return Update Query
	 */
	public static String getUpdateQuery(String schema, String tableName, List<String> columns, String conditionColumn,Object value) {
		if ((Utility.isBlank(schema) && Utility.isBlank(tableName)) && Utility.isBlank(columns)
				&& Utility.isBlank(conditionColumn))
			return null;
		String columnNames = "";
		for (String column : columns) {
			columnNames += "," + GRAVE + column + GRAVE + " = ?";
		}
		if(value instanceof String) {
			value = ",\""+value + "\"";
			value= ((String) value).substring(1);
		}
		return "UPDATE " + GRAVE + schema + GRAVE + "." + GRAVE + tableName + GRAVE + " SET " + columnNames.substring(1)
				+ " WHERE " + conditionColumn + " = "+value+";";
	}

	/**
	 * this method is used to generate query for delete operations
	 * 
	 * @author hari
	 * @param schema
	 * @param tableName
	 * @param conditionColumn
	 * @return delete query
	 */
	public static String getDeleteQuery(String schema, String tableName, String conditionColumn) {
		if ((Utility.isBlank(schema) && Utility.isBlank(tableName)) && Utility.isBlank(conditionColumn))
			return null;
		return "DELETE FROM " + GRAVE + schema + GRAVE + "." + GRAVE + tableName + GRAVE + " WHERE " + conditionColumn
				+ "  = ? ;";
	}

	/**
	 * this method is used to generate query for get the list of Column Data
	 * 
	 * @author hari
	 * @param schema
	 * @param tableName
	 * @param columns
	 * @return select query
	 */
	public static String getListQuery(String schema, String tableName, List<String> columns) {
		if ((Utility.isBlank(schema) && Utility.isBlank(tableName)) && Utility.isBlank(columns))
			return null;
		String columnName = "";
		for (String column : columns) {
			columnName += "," + GRAVE + column + GRAVE;
		}
		return "SELECT " + (columns.isEmpty() ? "*" : columnName.substring(1)) + " FROM " + GRAVE + schema + GRAVE + "."
				+ GRAVE + tableName + GRAVE + ";";
	}

	/**
	 * this method is used to generate query for get the row data
	 * 
	 * @author hari
	 * @param schema
	 * @param tableName
	 * @param columns
	 * @param conditionColumn
	 * @return select query
	 */
	public static String getQuery(String schema, String tableName, List<String> columns, String conditionColumn,
			String values) {
		if ((Utility.isBlank(schema) && Utility.isBlank(tableName)) && Utility.isBlank(columns))
			return null;
		String query = getListQuery(schema, tableName, columns);
		return query.substring(0, query.length() - 1) + " WHERE " + conditionColumn + " IN (" + values + ") ;";
	}

	public static String getQuerys(String schema, String tableName, List<String> columns, String conditionColumn,
			List<Object> values) {
		if ((Utility.isBlank(schema)) && (Utility.isBlank(tableName)) && (Utility.isBlank(columns)))
			return null;
		String query = getListQuery(schema, tableName, columns);
		String valueString ="";
		for (Object value : values) {
			valueString += (value instanceof String) ? ",\"" + value + "\"" : "," + value;
		}
		return query.substring(0, query.length() - 1) + " WHERE " + conditionColumn + " IN (" + (valueString.substring(1)) + ") ;";
	}
}
