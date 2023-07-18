package de.zeroco.jdbc;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import de.zeroco.utility.Utility;

public class JdbcTasks {

	public static final String DATABASE_URL = "jdbc:mysql://localhost:3306/zerocode?characterEncoding=utf8";
	public static final String USER = "admin";
	public static final String USER_PASSWORD = "@Chakri007";
	public static final String REGISTER_DRIVER = "com.mysql.jdbc.Driver";
	public static final String TABLE_NAME = "employee_table";

	public static void main(String[] args) {
//		System.out.println(getAddInfo("zerocode", "employee_table", Arrays.asList()));
//		System.out.println(getListValues(list, "pk_id"));
//		System.out.println(getListValues("zerocode", "students_table", Arrays.asList(), "student_name"));
//		List<Map<String, Object>> resultList = getAddInfo("zerocode", "students_table", Arrays.asList());
//	    System.out.println(getAddInfo(resultList));
//	   String query ="SELECT * FROM " + tableName + " WHERE "+ conditionColumn + " IN ( " + values + ") ;" ;

		List<Map<String, Object>> listOfMaps = new ArrayList<>();
		Map<String, Object> first = new LinkedHashMap<>();
		Map<String, Object> second = new LinkedHashMap<>();
		Map<String, Object> third = new LinkedHashMap<>();
		Map<String, Object> four = new LinkedHashMap<>();
		first.put("pk_id", 1);
		first.put("employee_name", "Hari Krishna");
		second.put("pk_id", 2);
		second.put("employee_name", "Ravan");
		third.put("pk_id", 3);
		third.put("employee_name", "Shiiva");
		four.put("pk_id", 4);
		four.put("employee_name", "Shiiva");
		listOfMaps.add(first);
		listOfMaps.add(second);
		listOfMaps.add(third);
		listOfMaps.add(four);
		System.out.println(getAddInfo(listOfMaps,"employee_name"));
	}

	public static List<Map<String, Object>> getAddInfo(List<Map<String, Object>> list) {
		if((Utility.isBlank(list))) return null;
		return getAddInfo(list, "pk_id");
	}

	public static String getListValues(List<Map<String, Object>> list, String key) {
		String values = "";
		for (int i = 0; i < list.size(); i++) {
			values += (list.get(i).get(key) instanceof String) ? ", \"" + list.get(i).get(key) + "\" " : "," + list.get(i).get(key);
		}
		values = values.substring(1);
		return values;
	}
	
	public static List<Map<String, Object>> getAddInfo(List<Map<String, Object>> list, String key) {
		if((Utility.isBlank(list)) && (Utility.isBlank(key))) return null;
		List<Map<String, Object>> data = new ArrayList<>();
		Map<String, Object> resource = new LinkedHashMap<>();
		Connection connect = DbUtility.getConnection(DATABASE_URL, USER, USER_PASSWORD);
		int countColumns = 0;
		String values = "";
		for (int i = 0; i < list.size(); i++) {
			values += (list.get(i).get(key) instanceof String) ? ", \"" + list.get(i).get(key) + "\" " : "," + list.get(i).get(key);
		}
		values = values.substring(1);
		String query = "SELECT * FROM " +" zerocode ."+ TABLE_NAME + " WHERE " + key + " IN ( " + values + ") ;";
		try {
			PreparedStatement statement = connect.prepareStatement(query);
			ResultSet set = statement.executeQuery();
			ResultSetMetaData metaData = set.getMetaData();
			countColumns = metaData.getColumnCount();
			while (set.next()) {
				Map<String, Object> map = new HashMap<>();
				for (int i = 1; i <= countColumns; i++) {
					map.put(metaData.getColumnName(i), set.getObject(i));
				}
				data.add(map);
			}
			resource.put("addInfo", data);
			list.add(resource);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			DbUtility.closeConnection(connect);
		}
		return list;
	}

}
