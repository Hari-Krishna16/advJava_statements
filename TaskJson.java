package de.zeroco.jdbc;

import java.io.IOException;
import java.util.List;

import de.zeroco.files.FileOperations;
import de.zeroco.utility.Utility;

public class TaskJson {

	public static final String DATABASE_URL = "jdbc:mysql://localhost:3306/zerocode?characterEncoding=utf8";
	public static final String USER = "admin";
	public static final String USER_PASSWORD = "@Chakri007";
	public static final String REGISTER_DRIVER = "com.mysql.jdbc.Driver";

	public static void main(String[] args) {
//		String path = "/home/hari/Documents/Task.txt";
		String filePath = "/home/hari/Documents/newTask.txt";
//		System.out.println(getFileData(path));
//		System.out.println(readingsFileData(filePath));
		System.out.println(generateJSONSchemaFromData(filePath));
	}

	public static String getJson(String filePath) {
		if ((Utility.isBlank(filePath))) {
			return null;
		}
		List<String> list;
		String result = "";
		try {
			list = FileOperations.readingFilesData(filePath);
			String json = "{\"fields\":{\"$field$\":{\"name\":\"$field$\",\"type\":\"$type$\"}}}";
			String data = json.substring(json.indexOf("$field$") - 1, json.lastIndexOf("$type$") + 8);
			for (int i = 0; i < list.size(); i++) {
				String[] word = list.get(i).split(",");
				result = result + ","
						+ data.replace("$field$", word[0].toLowerCase()).replace("$type$", word[1].toLowerCase());
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return "{\"fields\":{" + result.substring(1) + "}}";
	}

	public static String generateJSONSchemaFromData(String filePath) {
		 if((Utility.isBlank(filePath))) {
			 return null;
		 }
		 String values ="";
		  String[] fields = null;
		    try {
		        List<String> list = FileOperations.readingFilesData(filePath);
		        String json = "{\"fields\":{\"$field$\":{\"name\":\"$field$\",\"type\":\"$type$\"}}}";
		        String data = json.substring(json.indexOf("$field$") - 1, json.lastIndexOf("$type$") + 8);
		        for (int i = 1; i < list.size() - 1; i++) {
		             fields = list.get(i).split("\\s");
		             values += ","
								+ data.replace("$field$", fields[0].toLowerCase()).replace("$type$", fields[1].toLowerCase());
		            values = values.replaceAll("\\(\\d+\\)", "").replaceAll("\\bvarchar\\b(,)?|bpchar\\b(,)?", "text").replaceAll("\\bnumeric\\b(,)?|int4\\b(,)?", "number").replaceAll("\\bdate\\b(,)?|\\btimestamp\\b(,)?", "sqldatetime");
		        }
		    } catch (IOException e) {
		        e.printStackTrace();
		    }
		    return"{\"fields\":{" + values.substring(1) + "}}";
		}
}
