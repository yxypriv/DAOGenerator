package daogenerator.utils;

import java.lang.reflect.Field;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

public class SqlConstructUtil {
	public static <T> T _constructResult(List<Field> fieldList, Class<T> type, ResultSet rs) throws SQLException {
		T obj = null;
		try {
			obj = type.newInstance();
			for (Field f : fieldList) {
				f.setAccessible(true);
				if (f.getType().equals(String.class))
					f.set(obj, rs.getString(StringsBuildUtil.escapeSystemKeyword(f.getName(), false)));
				else if (f.getType().equals(Integer.class))
					f.set(obj, rs.getInt(StringsBuildUtil.escapeSystemKeyword(f.getName(), false)));
				else if (f.getType().equals(Short.class))
					f.set(obj, rs.getShort(StringsBuildUtil.escapeSystemKeyword(f.getName(), false)));
				else if (f.getType().equals(Double.class))
					f.set(obj, rs.getDouble(StringsBuildUtil.escapeSystemKeyword(f.getName(), false)));
				else if (f.getType().equals(Date.class))
					f.set(obj, rs.getDate(StringsBuildUtil.escapeSystemKeyword(f.getName(), false)));
			}
		} catch (InstantiationException e) {
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			e.printStackTrace();
		}
		return obj;
	}

	public static void __safeSetString(PreparedStatement ps, int parameterIndex, String value) throws SQLException {
		if (null == value)
			ps.setNull(parameterIndex, Types.VARCHAR);
		else
			ps.setString(parameterIndex, value);
	}

	public static void __safeSetInt(PreparedStatement ps, int parameterIndex, Integer value) throws SQLException {
		if (null == value)
			ps.setNull(parameterIndex, Types.INTEGER);
		else
			ps.setInt(parameterIndex, value);
	}

	public static void __safeSetShort(PreparedStatement ps, int parameterIndex, Short value) throws SQLException {
		if (null == value)
			ps.setNull(parameterIndex, Types.SMALLINT);
		else
			ps.setShort(parameterIndex, value);
	}

	public static void __safeSetDouble(PreparedStatement ps, int parameterIndex, Double value) throws SQLException {
		if (null == value)
			ps.setNull(parameterIndex, Types.FLOAT);
		else
			ps.setDouble(parameterIndex, value);
	}

	public static void __safeSetDate(PreparedStatement ps, int parameterIndex, Date value) throws SQLException {
		if (null == value)
			ps.setNull(parameterIndex, Types.FLOAT);
		else
			ps.setDate(parameterIndex, value);
	}

}
