package services.dao.nsfc_aminer_combine.dao;

import java.lang.reflect.Field;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import com.mysql.jdbc.Statement;

import services.dao.nsfc_aminer_combine.model.NsfcAminerCombinePublication;
import daogenerator.services.interfaces.Callback;
import daogenerator.utils.SqlConstructUtil;
import daogenerator.utils.StringsBuildUtil;
import daogenerator.utils.connectionPool.ConnectionPool;
import daogenerator.utils.connectionPool.DBConnection;

public class NsfcAminerCombinePublicationDAO {
	static ConnectionPool pool = ConnectionPool.getInstance();
	static NsfcAminerCombinePublicationDAO instance = null;

	public static NsfcAminerCombinePublicationDAO getInstance() {
		if (null == instance) {
			instance = new NsfcAminerCombinePublicationDAO("nsfc_aminer_combine");
		}
		return instance;
	}
	
	public static NsfcAminerCombinePublicationDAO getNewInstance(String dbName) {
		return new NsfcAminerCombinePublicationDAO(dbName);
	}
	
	private final String dbName;
	private NsfcAminerCombinePublicationDAO(String dbName) {
		this.dbName = dbName;
	}
	
	public void truncate() {
		String sql = String.format("TRUNCATE %s.publication", dbName);
		DBConnection conn = pool.getConnection();
		PreparedStatement ps = null;
		try {
			ps = conn.prepareStatement(sql);
			ps.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionPool.close(conn, ps);
		}
	}

	public int insertReturnId(NsfcAminerCombinePublication obj) {
		String sql = String.format("INSERT INTO %s.publication(id,title,type,jconf,startpage,endpage,year,isbn,publisher,authors,school,dblpeelink,dblplink,issearch,isuseradd,pubkey,isextracted,ncitation,updated,u_citation_gen) VALUES ", dbName);

		DBConnection conn = pool.getConnection();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			_constructPS(ps, obj, 0);
			ps.executeUpdate();
			rs = ps.getGeneratedKeys();
			if (rs.next())
				return rs.getInt(1);
			else
				return -1;
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionPool.close(conn, ps);
		}
		return -1;
	}

	public void insert(NsfcAminerCombinePublication obj) {
		String sql = String.format("INSERT INTO %s.publication(id,title,type,jconf,startpage,endpage,year,isbn,publisher,authors,school,dblpeelink,dblplink,issearch,isuseradd,pubkey,isextracted,ncitation,updated,u_citation_gen) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", dbName);

		DBConnection conn = pool.getConnection();
		PreparedStatement ps = null;
		try {
			ps = conn.prepareStatement(sql);
			_constructPS(ps, obj, 0);
			ps.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionPool.close(conn, ps);
		}
	}

	/**
	 * @deprecated using insertMultiple instead
	 */
	@Deprecated
	public void insertBatch(List<NsfcAminerCombinePublication> objList) {
		String sql = String.format("INSERT INTO %s.publication(id,title,type,jconf,startpage,endpage,year,isbn,publisher,authors,school,dblpeelink,dblplink,issearch,isuseradd,pubkey,isextracted,ncitation,updated,u_citation_gen) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", dbName);

		DBConnection conn = pool.getConnection();
		PreparedStatement ps = null;
		try {
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(sql);
			for (NsfcAminerCombinePublication obj : objList) {
				_constructPS(ps, obj, 0);
				ps.addBatch();
			}
			ps.executeBatch();
			conn.setAutoCommit(true);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionPool.close(conn, ps);
		}
	}

	public void insertMultiple(List<NsfcAminerCombinePublication> objList) {
		if (objList.size() == 0)
			return;
		StringBuilder sqlBuilder = new StringBuilder(String.format("INSERT INTO %s.publication(id,title,type,jconf,startpage,endpage,year,isbn,publisher,authors,school,dblpeelink,dblplink,issearch,isuseradd,pubkey,isextracted,ncitation,updated,u_citation_gen) VALUES ", dbName));
		for (int i = 0; i < objList.size(); i++) {
			if (i != 0)
				sqlBuilder.append(",");
			sqlBuilder.append("(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
		}
		String sql = sqlBuilder.toString();
		DBConnection conn = pool.getConnection();
		PreparedStatement ps = null;
		try {
			int indexCount = 0;
			ps = conn.prepareStatement(sql);
			for (NsfcAminerCombinePublication obj : objList) {
				indexCount = _constructPS(ps, obj, indexCount);
			}
			ps.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionPool.close(conn, ps);
		}
	}

	public void insertMultipleBatch(List<NsfcAminerCombinePublication> objList, int multipleSize) {
		if (objList.size() == 0)
			return;
		String sqlHead = String.format("INSERT INTO %s.publication(id,title,type,jconf,startpage,endpage,year,isbn,publisher,authors,school,dblpeelink,dblplink,issearch,isuseradd,pubkey,isextracted,ncitation,updated,u_citation_gen) VALUES ", dbName);
		StringBuilder sqlBuilder = new StringBuilder(sqlHead);
		for (int i = 0; i < multipleSize; i++) {
			if (i != 0)
				sqlBuilder.append(",");
			sqlBuilder.append("(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");
		}
		String sql = sqlBuilder.toString();
		DBConnection conn = pool.getConnection();
		PreparedStatement ps = null;
		int startIndex = 0;
		try {
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(sql);
			while (startIndex + multipleSize <= objList.size()) {
				int indexCount = 0;
				for (NsfcAminerCombinePublication obj : objList.subList(startIndex, startIndex + multipleSize)) {
					indexCount = _constructPS(ps, obj, indexCount);
				}
				ps.addBatch();
				startIndex += multipleSize;
			}
			ps.executeBatch();
			conn.setAutoCommit(true);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionPool.close(conn, ps);
		}
		if (startIndex < objList.size()) {
			insertMultiple(objList.subList(startIndex, objList.size()));
		}
	}

	/**
	 * @deprecated using insertMultipleWithLimitedWindow instead
	 */
	@Deprecated
	public void insertBatchWithLimitedWindow(List<NsfcAminerCombinePublication> objList, int windowSize) {
		int startIndex = 0;
		int displayCounter = 0;
		while (startIndex < objList.size()) {
			insertBatch(objList.subList(startIndex, Math.min(startIndex + windowSize, objList.size())));
			startIndex += windowSize;
			if (++displayCounter % 20 == 0)
				System.out.println(String.format("[inserting NsfcAminerCombinePublication] %d / %d", startIndex, objList.size()));
		}
	}

	@Deprecated
	public void insertMultipleWithLimitedWindow(List<NsfcAminerCombinePublication> objList, int windowSize) {
		int startIndex = 0;
		int displayCounter = 0;
		while (startIndex < objList.size()) {
			insertMultiple(objList.subList(startIndex, Math.min(startIndex + windowSize, objList.size())));
			startIndex += windowSize;
			if (++displayCounter % 20 == 0)
				System.out.println(String.format("[inserting NsfcAminerCombinePublication] %d / %d", startIndex, objList.size()));
		}
	}

	public void insertMultipleBatchWithLimitedWindow(List<NsfcAminerCombinePublication> objList, int batchWindowSize, int batchMount) {
		int startIndex = 0;
		long t0 = System.currentTimeMillis();
		while (startIndex < objList.size()) {
			insertMultipleBatch(objList.subList(startIndex, Math.min(startIndex + batchMount * batchWindowSize, objList.size())), batchWindowSize);
			startIndex += batchMount * batchWindowSize;
			System.out.println(String.format("[inserting TestDbtest] %d / %d, cost %d ms, total estimation %d ms", startIndex, objList.size(), //
					(System.currentTimeMillis() - t0), (System.currentTimeMillis() - t0) * objList.size() / startIndex));
		}
	}

	public void update(NsfcAminerCombinePublication obj) {
		String sql = String.format("UPDATE %s.publication SET id = ?,title = ?,type = ?,jconf = ?,startpage = ?,endpage = ?,year = ?,isbn = ?,publisher = ?,authors = ?,school = ?,dblpeelink = ?,dblplink = ?,issearch = ?,isuseradd = ?,pubkey = ?,isextracted = ?,ncitation = ?,updated = ?,u_citation_gen = ? WHERE id = ?", dbName);
		DBConnection conn = pool.getConnection();
		PreparedStatement ps = null;
		try {
			ps = conn.prepareStatement(sql.toString());
			int indexCount = _constructPS(ps, obj, 0);
			ps.setInt(++indexCount, obj.getId());
			ps.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionPool.close(conn, ps);
		}
	}

	public void updateBatch(List<NsfcAminerCombinePublication> objList) {
		String sql = String.format("UPDATE %s.publication SET id = ?,title = ?,type = ?,jconf = ?,startpage = ?,endpage = ?,year = ?,isbn = ?,publisher = ?,authors = ?,school = ?,dblpeelink = ?,dblplink = ?,issearch = ?,isuseradd = ?,pubkey = ?,isextracted = ?,ncitation = ?,updated = ?,u_citation_gen = ? WHERE id = ?", dbName);

		DBConnection conn = pool.getConnection();
		PreparedStatement ps = null;
		try {
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(sql);
			int count = 0;
			for (NsfcAminerCombinePublication obj : objList) {
				if(++count % 5000 == 0) 
					System.out.println(String.format("[batch updating NsfcAminerCombinePublication] %d / %d", count, objList.size()));
				int indexCount = _constructPS(ps, obj, 0);
				ps.setInt(++indexCount, obj.getId());
				ps.addBatch();
			}
			ps.executeBatch();
			conn.setAutoCommit(true);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionPool.close(conn, ps);
		}
	}
	
	public void updateBatchWithIntegerFieldSelection(String whereField, Integer whereParamCount, List<List<Integer>> whereInValueList, //
															 String setField, List<Integer> setToValueList) {
		if (null == whereInValueList || whereInValueList.size() == 0) {
			return ;
		} else if(whereParamCount != whereInValueList.get(0).size() || whereInValueList.size() != setToValueList.size()) {
			new Exception("Mismatch parameter size").printStackTrace();
			return;
		}

		StringBuilder sqlBuilder = new StringBuilder( //
			String.format("UPDATE %s.publication SET %s = ? WHERE %s IN (", dbName, setField, whereField));
		for (int i = 0; i < whereParamCount; i++) {
			sqlBuilder.append('?').append(',');
		}
		sqlBuilder.setCharAt(sqlBuilder.length() - 1, ')');

		String sql = sqlBuilder.toString();
		
		DBConnection conn = pool.getConnection();
		PreparedStatement ps = null;
		try {
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(sql);
			for(int i=0; i<whereInValueList.size(); i++) {
				SqlConstructUtil.__safeSetInt(ps, 1, setToValueList.get(i));
				for (int j = 0; j < whereParamCount; j++) {
					ps.setInt(j + 2, whereInValueList.get(i).get(j));
				}
				ps.addBatch();
			}
			ps.executeBatch();
			conn.setAutoCommit(true);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionPool.close(conn, ps);
		}
	}

	public void updateWithIntegerFieldSelection(String whereField, List<Integer> whereInValue, String setField, Integer setToValue) {
		if (null == whereInValue || whereInValue.size() == 0 ) {
			return ;
		}

		StringBuilder sqlBuilder = new StringBuilder( //
			String.format("UPDATE %s.publication SET %s = ? WHERE %s IN (", dbName, setField, whereField));
		for (int i = 0; i < whereInValue.size(); i++) {
			sqlBuilder.append('?').append(',');
		}
		sqlBuilder.setCharAt(sqlBuilder.length() - 1, ')');

		String sql = sqlBuilder.toString();
		
		DBConnection conn = pool.getConnection();
		PreparedStatement ps = null;
		try {
			ps = conn.prepareStatement(sql);
			SqlConstructUtil.__safeSetInt(ps, 1, setToValue);
			for (int i = 0; i < whereInValue.size(); i++) {
				ps.setInt(i + 2, whereInValue.get(i));
			}
			ps.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionPool.close(conn, ps);
		} 
	}

	public void delete(Integer id) {
		String sql = String.format("DELETE FROM %s.publication WHERE id = ?", dbName);

		DBConnection conn = pool.getConnection();
		PreparedStatement ps = null;
		try {
			ps = conn.prepareStatement(sql.toString());
			ps.setInt(1, id);
			ps.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionPool.close(conn, ps);
		}
	}

	@Deprecated
	public void deleteBatch(List<Integer> idList) {
		String sql = String.format("DELETE FROM %s.publication WHERE id = ?", dbName);

		DBConnection conn = pool.getConnection();
		PreparedStatement ps = null;
		try {
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(sql);
			int count = 0;
			for (Integer id : idList) {
				if(++count % 5000 == 0) 
					System.out.println(String.format("[batch deleting NsfcAminerCombinePublication] %d / %d", count, idList.size()));
				ps.setInt(1, id);
				ps.addBatch();
			}
			ps.executeBatch();
			conn.setAutoCommit(true);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionPool.close(conn, ps);
		}
	}

	public void deleteMultipleById(List<Integer> idList) {
		if (null == idList || idList.size() == 0 ) 
			return ;
			
		StringBuilder sqlBuilder = new StringBuilder("DELETE FROM %s.publication WHERE id IN (");
		for (int i = 0; i < idList.size(); i++) {
			sqlBuilder.append('?').append(',');
		}
		sqlBuilder.setCharAt(sqlBuilder.length() - 1, ')');

		String sql = String.format(sqlBuilder.toString(), dbName);

		DBConnection conn = pool.getConnection();
		PreparedStatement ps = null;
		try {
			ps = conn.prepareStatement(sql);
			for (int i = 0; i < idList.size(); i++) {
				ps.setInt(i + 1, idList.get(i));
			}
			ps.executeUpdate();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionPool.close(conn, ps);
		} 
	}
	
	public void deleteMultipleBatchById(List<Integer> idList, int multipleSize) {
		if (idList.size() == 0)
			return;
		StringBuilder sqlBuilder = new StringBuilder("DELETE FROM %s.publication WHERE id IN (");
		for (int i = 0; i < multipleSize; i++) {
			sqlBuilder.append('?').append(',');
		}
		sqlBuilder.setCharAt(sqlBuilder.length() - 1, ')');
		
		String sql = String.format(sqlBuilder.toString(), dbName);
		DBConnection conn = pool.getConnection();
		PreparedStatement ps = null;
		int startIndex = 0;
		try {
			conn.setAutoCommit(false);
			ps = conn.prepareStatement(sql);
			while (startIndex + multipleSize <= idList.size()) {
				List<Integer> subList = idList.subList(startIndex, startIndex + multipleSize);
				for (int i = 0; i < subList.size(); i++) {
					ps.setInt(i + 1, subList.get(i));
				}
				ps.addBatch();
				startIndex += multipleSize;
			}
			ps.executeBatch();
			conn.setAutoCommit(true);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionPool.close(conn, ps);
		}
		if (startIndex < idList.size()) {
			deleteMultipleById(idList.subList(startIndex, idList.size()));
		}
	}

	public void deleteMultipleBatchByIdWithLimitedWindow(List<Integer> idList, int batchWindowSize, int batchMount) {
		int startIndex = 0;
		long t0 = System.currentTimeMillis();
		while (startIndex < idList.size()) {
			deleteMultipleBatchById(idList.subList(startIndex, Math.min(startIndex + batchMount * batchWindowSize, idList.size())), batchWindowSize);
			startIndex += batchMount * batchWindowSize;
			System.out.println(String.format("[deleting TestDbtest] %d / %d, cost %d ms, total estimation %d ms", startIndex, idList.size(), //
					(System.currentTimeMillis() - t0), (System.currentTimeMillis() - t0) * idList.size() / startIndex));
		}
	}

	public Integer selectMaxId() {
		Integer result = null;
		String sql = String.format("SELECT MAX(id) FROM %s.publication", dbName);
		DBConnection conn = pool.getConnection();
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			ps = conn.prepareStatement(sql);
			rs = ps.executeQuery();
			if (rs.next()) {
				result = rs.getInt("MAX(id)");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionPool.close(conn, ps, rs);
		}
		return result;
	}

	public NsfcAminerCombinePublication selectById(Integer id) {
		if (null == id) {
			return null;
		}
		String sql = String.format("SELECT * FROM %s.publication WHERE id = ?", dbName);
		NsfcAminerCombinePublication result = null;
		DBConnection conn = pool.getConnection();
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			ps = conn.prepareStatement(sql);
			ps.setInt(1, id);
			rs = ps.executeQuery();
			if (rs.next()) {
				result = _constructResult(rs);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionPool.close(conn, ps, rs);
		}
		return result;
	}

	public List<NsfcAminerCombinePublication> selectByIdList(List<Integer> idList) {
		if (null == idList) {
			return null;
		} else if (idList.size() == 0) {
			return new ArrayList<NsfcAminerCombinePublication>();
		}

		StringBuilder sqlBuilder = new StringBuilder("SELECT * FROM %s.publication WHERE id IN (");
		for (int i = 0; i < idList.size(); i++) {
			sqlBuilder.append('?').append(',');
		}
		sqlBuilder.setCharAt(sqlBuilder.length() - 1, ')');

		String sql = String.format(sqlBuilder.toString(), dbName);

		List<NsfcAminerCombinePublication> result = new ArrayList<NsfcAminerCombinePublication>();
		DBConnection conn = pool.getConnection();
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			ps = conn.prepareStatement(sql);
			for (int i = 0; i < idList.size(); i++) {
				ps.setInt(i + 1, idList.get(i));
			}
			rs = ps.executeQuery();
			while (rs.next()) {
				result.add(_constructResult(rs));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionPool.close(conn, ps, rs);
		}
		return result;
	}

	public List<NsfcAminerCombinePublication> selectByIdListWithLimitedWindow(List<Integer> idList, Integer windowSize) {
		if (null == idList)
			return null;
		
		List<NsfcAminerCombinePublication> result = new ArrayList<NsfcAminerCombinePublication>();
		int paramterListStartIndex = 0;
		while (paramterListStartIndex < idList.size()) {
			System.out.println(String.format("[Selecting Id]\t%d / %d", paramterListStartIndex, idList.size()));
			List<Integer> partialIdList = new ArrayList<Integer>();
			int subListEnd = Math.min(paramterListStartIndex + windowSize, idList.size());
			partialIdList.addAll(idList.subList(paramterListStartIndex, subListEnd));
			result.addAll(selectByIdList(partialIdList));
			
			partialIdList.clear();
			paramterListStartIndex += windowSize;
		}
		
		return result;
	}

	public NsfcAminerCombinePublication selectSingleByStringField(String field, String value) {
		if (field == null || "".equals(field.trim())) {
			return null;
		}
		String sql = String.format("SELECT * FROM %s.publication WHERE %s = ? LIMIT 1", dbName, field);
		NsfcAminerCombinePublication result = null;
		DBConnection conn = pool.getConnection();
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			ps = conn.prepareStatement(sql);
			SqlConstructUtil.__safeSetString(ps, 1, value);
			rs = ps.executeQuery();
			if (rs.next()) {
				result = _constructResult(rs);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionPool.close(conn, ps, rs);
		}
		return result;
	}

	public List<NsfcAminerCombinePublication> selectByStringField(String field, String value) {
		if (field == null || "".equals(field.trim())) {
			return null;
		}
		String sql = String.format("SELECT * FROM %s.publication WHERE %s = ?", dbName, field);
		List<NsfcAminerCombinePublication> result = new ArrayList<NsfcAminerCombinePublication>();
		DBConnection conn = pool.getConnection();
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			ps = conn.prepareStatement(sql);
			SqlConstructUtil.__safeSetString(ps, 1, value);
			rs = ps.executeQuery();
			while (rs.next()) {
				result.add(_constructResult(rs));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionPool.close(conn, ps, rs);
		}
		return result;
	}

	public List<NsfcAminerCombinePublication> selectByMultipleStringField(String field, List<String> valueList) {
		if (null == valueList) {
			return null;
		} else if (valueList.size() == 0) {
			return new ArrayList<NsfcAminerCombinePublication>();
		}

		StringBuilder sqlBuilder = new StringBuilder(String.format("SELECT * FROM %s.publication WHERE %s IN (", dbName, field));
		for (int i = 0; i < valueList.size(); i++) {
			sqlBuilder.append('?').append(',');
		}
		sqlBuilder.setCharAt(sqlBuilder.length() - 1, ')');

		String sql = sqlBuilder.toString();

		List<NsfcAminerCombinePublication> result = new ArrayList<NsfcAminerCombinePublication>();
		DBConnection conn = pool.getConnection();
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			ps = conn.prepareStatement(sql);
			for (int i = 0; i < valueList.size(); i++) {
				SqlConstructUtil.__safeSetString(ps, i + 1, valueList.get(i));
			}
			rs = ps.executeQuery();
			while (rs.next()) {
				result.add(_constructResult(rs));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionPool.close(conn, ps, rs);
		}
		return result;
	}

	public List<NsfcAminerCombinePublication> selectByMultipleStringFieldWithLimitedWindow(String field, List<String> valueList, Integer windowSize) {
		if (null == valueList)
			return null;

		List<NsfcAminerCombinePublication> result = new ArrayList<NsfcAminerCombinePublication>();
		int paramterListStartIndex = 0;
		while (paramterListStartIndex < valueList.size()) {
			System.out.println(String.format("[Selecting String Field \"%s\"]\t%d / %d", field, paramterListStartIndex, valueList.size()));
			List<String> partialIdList = new ArrayList<String>();
			int subListEnd = Math.min(paramterListStartIndex + windowSize, valueList.size());
			partialIdList.addAll(valueList.subList(paramterListStartIndex, subListEnd));
			result.addAll(selectByMultipleStringField(field, partialIdList));
			
			partialIdList.clear();
			paramterListStartIndex += windowSize;
		}

		return result;
	}

	public NsfcAminerCombinePublication selectSingleByIntegerField(String field, Integer value) {
		if (field == null || "".equals(field.trim())) {
			return null;
		}
		String sql = String.format("SELECT * FROM %s.publication WHERE %s = ? LIMIT 1", dbName, field);
		NsfcAminerCombinePublication result = null;
		DBConnection conn = pool.getConnection();
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			ps = conn.prepareStatement(sql);
			SqlConstructUtil.__safeSetInt(ps, 1, value);
			rs = ps.executeQuery();
			if (rs.next()) {
				result = _constructResult(rs);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionPool.close(conn, ps, rs);
		}
		return result;
	}

	public List<NsfcAminerCombinePublication> selectByIntegerField(String field, Integer value) {
		if (field == null || "".equals(field.trim())) {
			return null;
		}
		String sql = String.format("SELECT * FROM %s.publication WHERE %s = ?", dbName, field);
		List<NsfcAminerCombinePublication> result = new ArrayList<NsfcAminerCombinePublication>();
		DBConnection conn = pool.getConnection();
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			ps = conn.prepareStatement(sql);
			SqlConstructUtil.__safeSetInt(ps, 1, value);
			rs = ps.executeQuery();
			while (rs.next()) {
				result.add(_constructResult(rs));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionPool.close(conn, ps, rs);
		}
		return result;
	}

	public List<NsfcAminerCombinePublication> selectByMultipleIntegerField(String field, List<Integer> valueList) {
		if (null == valueList) {
			return null;
		} else if (valueList.size() == 0) {
			return new ArrayList<NsfcAminerCombinePublication>();
		}

		StringBuilder sqlBuilder = new StringBuilder(String.format("SELECT * FROM %s.publication WHERE %s IN (", dbName, field));
		for (int i = 0; i < valueList.size(); i++) {
			sqlBuilder.append('?').append(',');
		}
		sqlBuilder.setCharAt(sqlBuilder.length() - 1, ')');

		String sql = sqlBuilder.toString();

		List<NsfcAminerCombinePublication> result = new ArrayList<NsfcAminerCombinePublication>();
		DBConnection conn = pool.getConnection();
		PreparedStatement ps = null;
		ResultSet rs = null;

		try {
			ps = conn.prepareStatement(sql);
			for (int i = 0; i < valueList.size(); i++) {
				SqlConstructUtil.__safeSetInt(ps, i+1, valueList.get(i));
			}
			rs = ps.executeQuery();
			while (rs.next()) {
				result.add(_constructResult(rs));
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionPool.close(conn, ps, rs);
		}
		return result;
	}

	public List<NsfcAminerCombinePublication> selectByMultipleIntegerFieldWithLimitedWindow(String field, List<Integer> valueList, Integer windowSize) {
		if (null == valueList)
			return null;

		List<NsfcAminerCombinePublication> result = new ArrayList<NsfcAminerCombinePublication>();
		int paramterListStartIndex = 0;
		while (paramterListStartIndex < valueList.size()) {
			System.out.println(String.format("[Selecting Integer Field \"%s\"]\t%d / %d", field, paramterListStartIndex, valueList.size()));
			List<Integer> partialIdList = new ArrayList<Integer>();
			int subListEnd = Math.min(paramterListStartIndex + windowSize, valueList.size());
			partialIdList.addAll(valueList.subList(paramterListStartIndex, subListEnd));
			result.addAll(selectByMultipleIntegerField(field, partialIdList));

			partialIdList.clear();
			paramterListStartIndex += windowSize;
		}

		return result;
	}


	public List<NsfcAminerCombinePublication> walk(List<Field> fieldList, int start, int limit) {
		List<NsfcAminerCombinePublication> result = new ArrayList<NsfcAminerCombinePublication>();

		boolean defaultField = null == fieldList || fieldList.size() == 0;
		String fieldString = "*";
		if (!defaultField) {
			StringBuilder fieldStringBuilder = new StringBuilder();
			fieldStringBuilder.append("id, ");
			for (Field field : fieldList)
				fieldStringBuilder.append(StringsBuildUtil.escapeSystemKeyword(field.getName(), true)).append(", ");
			fieldStringBuilder.delete(fieldStringBuilder.length() - 2, fieldStringBuilder.length() - 1);
			fieldString = fieldStringBuilder.toString();
		}
		String sql = String.format("SELECT %s FROM %s.publication WHERE id >= ? ORDER BY id LIMIT ?", fieldString, dbName);

		DBConnection conn = pool.getConnection();
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = conn.prepareStatement(sql);
			ps.setInt(1, start);
			ps.setInt(2, limit);
			rs = ps.executeQuery();
			while (rs.next()) {
				if(defaultField)
					result.add(_constructResult(rs));
				else {
					NsfcAminerCombinePublication obj = SqlConstructUtil._constructResult(fieldList, NsfcAminerCombinePublication.class, rs);
					obj.setId(rs.getInt("id"));
					result.add(obj);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			ConnectionPool.close(conn, ps, rs);
		}
		return result;
	}

	public List<NsfcAminerCombinePublication> walk(int start, int limit) {
		return walk(null, start, limit);
	}

	public List<NsfcAminerCombinePublication> walkAll() {
		return walkAll(null, -1);
	}

	public List<NsfcAminerCombinePublication> walkAll(int walkstep) {
		return walkAll(null, walkstep);
	}
	
	/**
	 * walk given size of data from DB.
	 * 
	 * @param walkstep
	 *            the number of step, -1 stands for all data, each step contains 500 lines.
	 */

	public List<NsfcAminerCombinePublication> walkAll(List<Field> fieldList, int walkstep) {
		long t0 = System.currentTimeMillis();
		int stepCount = 0;
		List<NsfcAminerCombinePublication> result = new ArrayList<NsfcAminerCombinePublication>();
		int startid = 0;
		int limit = 500;
		while (true) {
			List<NsfcAminerCombinePublication> walk = walk(startid, limit);
			if (null == walk || walk.size() == 0)
				break;
			result.addAll(walk);
			startid = walk.get(walk.size() - 1).getId() + 1;
			stepCount++;
			if (stepCount % 20 == 0) {
				System.out.println(String.format("[Loading NsfcAminerCombinePublication] id:%d, timeUsed:%dms", startid, (System.currentTimeMillis() - t0)));
			}
			if (-1 != walkstep && stepCount >= walkstep) {
				break;
			}
		}
		return result;
	}

	public void fetchAll(Callback<NsfcAminerCombinePublication> callback) {
		fetchAll(callback, -1);
	}

	public void fetchAll(Callback<NsfcAminerCombinePublication> callback, int fetchstep) {
		fetchAll(callback, null, fetchstep);
	}
	
	public void fetchAll(Callback<NsfcAminerCombinePublication> callback, List<Field> fieldList) {
		fetchAll(callback, fieldList, -1);
	}
	
	public void fetchAll(Callback<NsfcAminerCombinePublication> callback, List<Field> fieldList, int fetchstep) {
		long t0 = System.currentTimeMillis();
		int stepCount = 0;
		int startid = 0;
		int limit = 500;
		while (true) {
			List<NsfcAminerCombinePublication> walk = walk(fieldList, startid, limit);
			if (null == walk || walk.size() == 0)
				break;

			callback.process(walk);

			startid = walk.get(walk.size() - 1).getId() + 1;
			stepCount++;
			if (stepCount % 20 == 0) {
				System.out.println(String.format("[Fetching NsfcAminerCombinePublication] id:%d, timeUsed:%dms", startid, (System.currentTimeMillis() - t0)));
			}
			if (-1 != fetchstep && stepCount >= fetchstep) {
				break;
			}
		}
	}

	public List<Field> _getFieldList(String[] fieldNameArray) throws NoSuchFieldException {
		List<Field> result = new ArrayList<Field>();
		try {
			for (String fieldName : fieldNameArray) {
				result.add(NsfcAminerCombinePublication.class.getDeclaredField(fieldName));
			}
		} catch (NoSuchFieldException e) {
			throw e;
		} catch (SecurityException e) {
			e.printStackTrace();
		}
		return result;
	}

	public int _constructPS(PreparedStatement ps, NsfcAminerCombinePublication obj, int indexCount) throws SQLException {
		SqlConstructUtil.__safeSetInt(ps, ++indexCount, obj.getId());
		SqlConstructUtil.__safeSetString(ps, ++indexCount, obj.getTitle());
		SqlConstructUtil.__safeSetInt(ps, ++indexCount, obj.getType());
		SqlConstructUtil.__safeSetInt(ps, ++indexCount, obj.getJconf());
		SqlConstructUtil.__safeSetInt(ps, ++indexCount, obj.getStartpage());
		SqlConstructUtil.__safeSetInt(ps, ++indexCount, obj.getEndpage());
		SqlConstructUtil.__safeSetInt(ps, ++indexCount, obj.getYear());
		SqlConstructUtil.__safeSetString(ps, ++indexCount, obj.getIsbn());
		SqlConstructUtil.__safeSetInt(ps, ++indexCount, obj.getPublisher());
		SqlConstructUtil.__safeSetString(ps, ++indexCount, obj.getAuthors());
		SqlConstructUtil.__safeSetString(ps, ++indexCount, obj.getSchool());
		SqlConstructUtil.__safeSetString(ps, ++indexCount, obj.getDblpeelink());
		SqlConstructUtil.__safeSetString(ps, ++indexCount, obj.getDblplink());
		SqlConstructUtil.__safeSetInt(ps, ++indexCount, obj.getIssearch());
		SqlConstructUtil.__safeSetInt(ps, ++indexCount, obj.getIsuseradd());
		SqlConstructUtil.__safeSetString(ps, ++indexCount, obj.getPubkey());
		SqlConstructUtil.__safeSetInt(ps, ++indexCount, obj.getIsextracted());
		SqlConstructUtil.__safeSetInt(ps, ++indexCount, obj.getNcitation());
		SqlConstructUtil.__safeSetShort(ps, ++indexCount, obj.getUpdated());
		SqlConstructUtil.__safeSetInt(ps, ++indexCount, obj.getUCitationGen());

		return indexCount;
	}

	public NsfcAminerCombinePublication _constructResult(ResultSet rs) throws SQLException {
		NsfcAminerCombinePublication obj = new NsfcAminerCombinePublication();
		obj.setId(rs.getInt("id"));
		obj.setTitle(rs.getString("title"));
		obj.setType(rs.getInt("type"));
		obj.setJconf(rs.getInt("jconf"));
		obj.setStartpage(rs.getInt("startpage"));
		obj.setEndpage(rs.getInt("endpage"));
		obj.setYear(rs.getInt("year"));
		obj.setIsbn(rs.getString("isbn"));
		obj.setPublisher(rs.getInt("publisher"));
		obj.setAuthors(rs.getString("authors"));
		obj.setSchool(rs.getString("school"));
		obj.setDblpeelink(rs.getString("dblpeelink"));
		obj.setDblplink(rs.getString("dblplink"));
		obj.setIssearch(rs.getInt("issearch"));
		obj.setIsuseradd(rs.getInt("isuseradd"));
		obj.setPubkey(rs.getString("pubkey"));
		obj.setIsextracted(rs.getInt("isextracted"));
		obj.setNcitation(rs.getInt("ncitation"));
		obj.setUpdated(rs.getShort("updated"));
		obj.setUCitationGen(rs.getInt("u_citation_gen"));

		return obj;
	}
}

