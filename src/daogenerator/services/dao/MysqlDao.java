package daogenerator.services.dao;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import daogenerator.services.dao.model.MysqlColumn;
import daogenerator.utils.connectionPool.ConnectionPool;
import daogenerator.utils.connectionPool.DBConnection;
/**
 * Contain basic Mysql methods.
 * @author wenqili
 */
public class MysqlDao {
	static ConnectionPool pool = ConnectionPool.getInstance();

	public static List<MysqlColumn> showColumnsFromTable(String dbName, String tableName) {
		List<MysqlColumn> result = new ArrayList<MysqlColumn>();
		String sql = String.format("SHOW COLUMNS FROM %s.%s", dbName, tableName);
		DBConnection conn = pool.getConnection();
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			ps = conn.prepareStatement(sql);
			// ps.setString(1, tableName);
			System.out.println(ps);
			rs = ps.executeQuery();
			while (rs.next()) {
				result.add(_construct(rs));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionPool.close(conn, ps, rs);
		}

		return result;
	}

	private static MysqlColumn _construct(ResultSet rs) throws SQLException {
		MysqlColumn result = new MysqlColumn();
		result.setField(rs.getString("Field"));
		result.setType(rs.getString("Type"));
		result.setNull(rs.getString("Null"));
		result.setKey(rs.getString("Key"));
		result.setDefault(rs.getString("Default"));
		result.setExtra(rs.getString("Extra"));
		return result;
	}
}
