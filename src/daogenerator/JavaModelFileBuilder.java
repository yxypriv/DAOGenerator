package daogenerator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import daogenerator.services.dao.model.MysqlColumn;
import daogenerator.utils.StringsBuildUtil;

/**
 * 
 */
public class JavaModelFileBuilder {
	// static final String RootPath = ConsistanceService.get("project.src.path");

	static Map<String, String> mysqlType2JavaTypeMap = new HashMap<String, String>();
	static Map<String, String> javaImportClassMap = new HashMap<String, String>();
	static {
		mysqlType2JavaTypeMap.put("varchar", "String");
		mysqlType2JavaTypeMap.put("char", "String");
		mysqlType2JavaTypeMap.put("text", "String");
		mysqlType2JavaTypeMap.put("mediumtext", "String");
		mysqlType2JavaTypeMap.put("longtext", "String");
		mysqlType2JavaTypeMap.put("int", "Integer");
		mysqlType2JavaTypeMap.put("smallint", "Short");
		mysqlType2JavaTypeMap.put("bigint", "Integer");
		mysqlType2JavaTypeMap.put("tinyint", "Short");
		mysqlType2JavaTypeMap.put("float", "Double");
		mysqlType2JavaTypeMap.put("double", "Double");
		mysqlType2JavaTypeMap.put("datetime", "Date");

		javaImportClassMap.put("Date", "java.sql.Date");
	}

	private String rootFilePath;
	private final String rootPackagePath;

	public JavaModelFileBuilder() {
		rootFilePath = JavaModelFileBuilder.class.getResource("/").getPath();
		String[] knownSuffix = new String[] {"/bin/", //
				"/WebRoot/WEB-INF/classes/"};
		boolean suffixFound = false;
		for (String suffix : knownSuffix) {
			if (rootFilePath.endsWith(suffix)) {
				rootFilePath = rootFilePath.substring(0, rootFilePath.length()-suffix.length()) + "/src/";
				suffixFound = true;
			}
		}
		if (!suffixFound) {
			new Exception(String.format("[unexpected resourcePath %s]", rootFilePath)).printStackTrace();
			System.exit(-1);
		}
		rootPackagePath = "services.dao";
	}

	/**
	 * @param projectSourcePath
	 *            the 'src' folder file path of your project.
	 * @param rootPackagePath
	 *            the inner package declaration of the files you want to put.
	 */
	public JavaModelFileBuilder(String projectSourcePath, String rootPackagePath) {
		super();
		this.rootFilePath = projectSourcePath;
		char lastChar = rootFilePath.charAt(rootFilePath.length() - 1);
		if (!(lastChar == '\\' || lastChar == '/')) {
			rootFilePath = rootFilePath + '/';
		}
		this.rootPackagePath = rootPackagePath;
	}

	/**
	 * 
	 * @param simplePackageName
	 * @param name
	 * @param columns
	 * @param dbName
	 * @param prefix
	 */
	public void outputFile(String simplePackageName, String name, List<MysqlColumn> columns, String dbName, String prefix) {
		outputJavaEntityFile(String.format("%s.%s.model", rootPackagePath, simplePackageName), name, columns, prefix);
		outputJavaDaoFile(String.format("%s.%s.dao", rootPackagePath, simplePackageName), //
				String.format("%s.%s.model", rootPackagePath, simplePackageName), name, columns, dbName, prefix);

	}

	/**
	 * Output the DAO file, using DAO platform
	 * 
	 * @param packageName
	 *            name of DAO package
	 * @param entityPackageName
	 * @param name
	 * @param columns
	 * @param dbName
	 * @param prefix
	 */
	private void outputJavaDaoFile(String packageName, String entityPackageName, String name, List<MysqlColumn> columns, String dbName, String prefix) {
		String relativePath = packageName.replaceAll("\\.", "/");
		File folder = null;
		folder = new File(rootFilePath + relativePath);

		if (!folder.exists()) {
			if (null != folder.getParentFile()) {
				folder.mkdirs();
			}
		}
		File file = new File(folder, StringsBuildUtil.toCamelCase(prefix + "_" + name) + "DAO.java");
		if (file.exists())
			file.delete();

		System.out.println(">>" + file.getAbsolutePath());

		// TODO: add import
		List<String> importClassList = new ArrayList<String>();
		List<String[]> javaFieldInformation = extractJavaFieldInformation(columns, importClassList);

		InputStream resourceAsStream = JavaModelFileBuilder.class.getResourceAsStream("/DAOPlatform");
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new InputStreamReader(resourceAsStream, "utf-8"));
		} catch (UnsupportedEncodingException e2) {
			e2.printStackTrace();
		}
		StringBuilder sb = new StringBuilder();
		String line = null;
		try {
			while ((line = reader.readLine()) != null) {
				sb.append(line).append("\r\n");
			}
		} catch (IOException e1) {
			e1.printStackTrace();
		}
		// till now, platform read as sb;
		List<String[]> replacement = new ArrayList<String[]>();
		replacement.add(new String[] { "EntityPackage", entityPackageName + "." + StringsBuildUtil.toCamelCase(prefix + "_" + name) });
		replacement.add(new String[] { "DAOPackage", packageName });
		replacement.add(new String[] { "EntityName", StringsBuildUtil.toCamelCase(prefix + "_" + name) });
		replacement.add(new String[] { "SimpleSqlTable", new StringBuilder().append(name).toString() });
		replacement.add(new String[] { "SqlTable", new StringBuilder().append("%s.").append(name).toString() });
		replacement.add(new String[] { "DbName", dbName });
		{
			StringBuilder selectedImport = new StringBuilder();
			for (String importClass : importClassList) {
				selectedImport.append("import ").append(importClass).append(";\r\n");
			}
			replacement.add(new String[] { "SelectedImports", selectedImport.toString() });
		}

		{
			StringBuilder sqlInsert = new StringBuilder();
			StringBuilder sqlQuestionMarkBracket = new StringBuilder();
			sqlInsert.append("INSERT INTO %s.").append(name).append("(");
			for (String[] typePair : javaFieldInformation) {
				sqlInsert.append(StringsBuildUtil.escapeSystemKeyword(typePair[1], true)).append(",");
			}
			sqlInsert.setCharAt(sqlInsert.length() - 1, ')');
			sqlInsert.append(" VALUES ");
			{
				sqlQuestionMarkBracket.append("(");
				for (int i = 0; i < javaFieldInformation.size(); i++) {
					sqlQuestionMarkBracket.append("?,");
				}
				sqlQuestionMarkBracket.setCharAt(sqlQuestionMarkBracket.length() - 1, ')');
			}
			replacement.add(new String[] { "SqlInsert", sqlInsert.toString() });
			replacement.add(new String[] { "sqlQuestionMarkBracket", sqlQuestionMarkBracket.toString() });
		}
		{
			StringBuilder sqlUpdate = new StringBuilder();
			sqlUpdate.append("UPDATE %s.").append(name).append(" SET ");
			for (String[] typePair : javaFieldInformation) {
				sqlUpdate.append(StringsBuildUtil.escapeSystemKeyword(typePair[1], true)).append(" = ?,");
			}
			sqlUpdate.setCharAt(sqlUpdate.length() - 1, ' ');
			sqlUpdate.append("WHERE id = ?");
			replacement.add(new String[] { "SqlUpdate", sqlUpdate.toString() });
		}
		{
			StringBuilder sqlDelete = new StringBuilder();
			sqlDelete.append("DELETE FROM %s.").append(name).append(" WHERE id = ?");
			replacement.add(new String[] { "SqlDelete", sqlDelete.toString() });
		}
		{
			StringBuilder sqlSelect = new StringBuilder();
			sqlSelect.append("SELECT * FROM %s.").append(name);
			replacement.add(new String[] { "SqlSelect", sqlSelect.toString() });
		}

		{
			StringBuilder codeConstructPS = new StringBuilder();
			for (String[] typePair : javaFieldInformation) {
				String jdbcType = typePair[0];
				if (jdbcType.equals("Integer"))
					jdbcType = "Int";
				codeConstructPS.append(String.format("\t\tSqlConstructUtil.__safeSet%s(ps, ++indexCount, obj.get%s());\r\n", jdbcType, StringsBuildUtil.toCamelCase(typePair[1])));
			}
			replacement.add(new String[] { "CodeConstructPS", codeConstructPS.toString() });
		}
		{
			StringBuilder codeConstructResult = new StringBuilder();
			for (String[] typePair : javaFieldInformation) {
				String jdbcType = typePair[0];
				if (jdbcType.equals("Integer"))
					jdbcType = "Int";
				codeConstructResult.append(String.format("\t\tobj.set%s(rs.get%s(\"%s\"));\r\n",//
						StringsBuildUtil.toCamelCase(typePair[1]), jdbcType, StringsBuildUtil.escapeSystemKeyword(typePair[1], false)));
			}
			replacement.add(new String[] { "CodeConstructResult", codeConstructResult.toString() });
		}
		String fullText = sb.toString();
		for (String[] replace : replacement) {
			fullText = fullText.replaceAll(String.format("%%\\$\\{%s\\}%%", replace[0]), replace[1]);
		}

		PrintWriter writer = null;
		try {
			writer = new PrintWriter(file);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		writer.println(fullText);
		writer.close();
		try {
			reader.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Output the java entity file
	 * 
	 * @param packageName
	 * @param name
	 * @param columns
	 * @param prefix
	 */
	private void outputJavaEntityFile(String packageName, String name, List<MysqlColumn> columns, String prefix) {
		String relativePath = packageName.replaceAll("\\.", "/");
		File folder = null;
		folder = new File(rootFilePath + relativePath);

		if (!folder.exists()) {
			if (null != folder.getParentFile()) {
				folder.mkdirs();
			}
		}
		File file = new File(folder, StringsBuildUtil.toCamelCase(prefix + "_" + name) + ".java");
		if (file.exists())
			file.delete();

		List<String> importClassList = new ArrayList<String>();
		List<String[]> javaFieldInformation = extractJavaFieldInformation(columns, importClassList);

		PrintWriter writer = null;
		try {
			writer = new PrintWriter(file);
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
		writer.println("package " + packageName + ";");
		writer.println();
		for (String importClass : importClassList) {
			writer.println(String.format("import %s;", importClass));
		}
		writer.println();
		writer.println("public class " + StringsBuildUtil.toCamelCase(prefix + "_" + name) + " {");

		for (String[] typePair : javaFieldInformation) {
			writer.println(String.format("\t%s %s;", typePair[0], typePair[1]));
		}
		for (String[] typePair : javaFieldInformation) {
			writer.println(String.format("\tpublic %s get%s() {", typePair[0], StringsBuildUtil.toCamelCase(typePair[1])));
			writer.println(String.format("\t\treturn this.%s;", typePair[1]));
			writer.println(String.format("\t}"));
		}
		for (String[] typePair : javaFieldInformation) {
			writer.println(String.format("\tpublic void set%s(%s %s) {", StringsBuildUtil.toCamelCase(typePair[1]), typePair[0], typePair[1]));
			writer.println(String.format("\t\tthis.%s = %s;", typePair[1], typePair[1]));
			writer.println(String.format("\t}"));
		}

		writer.println("}");
		writer.close();
	}

	/**
	 * @return a list of String[2] where 0 stands for Field type and 1 stands for Field name
	 */
	private static List<String[]> extractJavaFieldInformation(List<MysqlColumn> columns, List<String> importClasses) {
		List<String[]> javaFieldsAndTypes = new ArrayList<String[]>();
		for (MysqlColumn mc : columns) {
			Pattern pattern = Pattern.compile("([^\\(]+)\\(");
			Matcher matcher = pattern.matcher(mc.getType());
			String mysqlType = null;
			if (matcher.find()) {
				mysqlType = matcher.group(1);
			} else {
				mysqlType = mc.getType();
			}
			String javaType = mysqlType2JavaTypeMap.get(mysqlType);
			if (javaImportClassMap.containsKey(javaType))
				importClasses.add(javaImportClassMap.get(javaType));
			if (null == javaType) {
				System.err.println("Unexpected Type :" + mc.getType());
				try {
					throw new Exception();
				} catch (Exception e) {
					e.printStackTrace();
				}
				System.exit(1);
			}
			javaFieldsAndTypes.add(new String[] { javaType, StringsBuildUtil.reservedWordEscape(mc.getField()) });

		}
		return javaFieldsAndTypes;
	}

}
