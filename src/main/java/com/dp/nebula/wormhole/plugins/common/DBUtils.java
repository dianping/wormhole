package com.dp.nebula.wormhole.plugins.common;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.nebula.wormhole.common.JobStatus;
import com.dp.nebula.wormhole.common.WormholeException;

public final class DBUtils {
	
	private static Log logger = LogFactory.getLog(DBUtils.class);
		
	private DBUtils() {
	}
	
	public static void dbPreCheck(String preSql,Connection conn) {
		int flag = -1;
		ResultSet rs = null;
		try {
			logger.info("pre check sql:" + preSql);
			rs = DBUtils.query(conn, preSql);
			rs.next();
			flag = rs.getInt(1);
		} catch (Exception e) {
			logger.error("Pre check sql has error" );
			throw new WormholeException(e,JobStatus.PRE_CHECK_FAILED.getStatus());	
		} finally {
            if (null != rs) {
			    try {
					DBUtils.closeResultSet(rs);
				} catch (SQLException e) {
					throw new WormholeException(e,JobStatus.PRE_CHECK_FAILED.getStatus());	
				}
            }         
		}
		if(flag != 1) {
			logger.error("Pre check condition is not satisfied." );
			throw new WormholeException(JobStatus.PRE_CHECK_FAILED.getStatus());
		}
	}

	public static ResultSet query(Connection conn, String sql) throws SQLException {
		Statement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
				ResultSet.CONCUR_READ_ONLY);
		return stmt.executeQuery(sql);
	}
	
	public static int update(Connection conn, String sql) throws SQLException {
		Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
				ResultSet.CONCUR_UPDATABLE);
		int rs = stmt.executeUpdate(sql);
		stmt.close();
		return rs;
	}
	

	public static void closeResultSet(ResultSet rs) throws SQLException {
		if (null != rs) {
			Statement stmt = rs.getStatement();
			if (null != stmt) {
				stmt.close();
				stmt = null;
			}
			rs.close();
		}
	}

	public static MetaData genMetaData(Connection conn, String sql)
			throws SQLException {
		MetaData meta = new MetaData();
		List<MetaData.Column> columns = new ArrayList<MetaData.Column>();

		ResultSet resultSet = null;
		try {
			resultSet = query(conn, sql);
			int columnCount = resultSet.getMetaData().getColumnCount();
			for (int i = 1; i <= columnCount; i++) {
				MetaData.Column col = meta.new Column();
				col.setColName(resultSet.getMetaData().getColumnName(i)
						.toLowerCase().trim());
				col.setDataType(resultSet.getMetaData().getColumnTypeName(i)
						.toLowerCase().trim());
				columns.add(col);
			}
			meta.setColInfo(columns);
			meta.setTableName(resultSet.getMetaData().getTableName(1).toLowerCase());
		} finally {
			closeResultSet(resultSet);
		}
		return meta;
	}
	
}