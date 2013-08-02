package com.dp.nebula.wormhole.plugins.reader.hivereader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import com.dp.nebula.wormhole.common.JobStatus;
import com.dp.nebula.wormhole.common.WormholeException;
import com.dp.nebula.wormhole.common.interfaces.ILine;
import com.dp.nebula.wormhole.common.interfaces.ILineSender;
import com.dp.nebula.wormhole.common.interfaces.IPluginMonitor;

public class HiveJdbcClient {
	private static final Logger LOG = Logger.getLogger(HiveJdbcClient.class);
	private static final String HIVE_JDBC_DRIVER_NAME = "org.apache.hadoop.hive.jdbc.HiveDriver";
	private static final String SET_REDUCE_NUMBER_COMMAND = "set mapred.reduce.tasks=";

	private String path;
	private String username;
	private String password;
	private String sql;
	private Connection conn;
	private Statement stmt;

	public void initialize() {
		try {
			Class.forName(HIVE_JDBC_DRIVER_NAME);
			conn = DriverManager.getConnection(this.path, this.username,
					this.password);
			stmt = conn.createStatement();
		} catch (ClassNotFoundException ce) {
			LOG.error("hive driver class not found, " + ce.getMessage());
			throw new WormholeException(ce,
					JobStatus.READ_CONNECTION_FAILED.getStatus());
		} catch (SQLException se) {
			LOG.error("hive get connection error, " + se.getMessage());
			throw new WormholeException(se,
					JobStatus.READ_CONNECTION_FAILED.getStatus());
		}
	}

	public void close() {
		try {
			if (stmt != null) {
				stmt.close();
			}
			if (conn != null) {
				conn.close();
			}
		} catch (SQLException e) {
			LOG.warn(e);
		}
	}

	public void processInsertQuery(int reduceNumber) throws SQLException {
		if (reduceNumber > 0) {
			LOG.info(SET_REDUCE_NUMBER_COMMAND + reduceNumber);
			stmt.execute(SET_REDUCE_NUMBER_COMMAND + reduceNumber);
		}
		LOG.info("hive execute insert sql:" + this.sql);
		ResultSet rs = stmt.executeQuery(this.sql);
		printMetaDataInfoAndGetColumnCount(rs);
		rs.close();
		stmt.close();
	}

	public void processSelectQuery(ILineSender lineSender,
			IPluginMonitor monitor) throws SQLException {
		LOG.info("hive execute select sql:" + this.sql);
		ResultSet rs = stmt.executeQuery(this.sql);
		int columnCount = printMetaDataInfoAndGetColumnCount(rs);
		while (rs.next()) {
			ILine oneLine = lineSender.createNewLine();
			for (int i = 1; i <= columnCount; i++) {
				oneLine.addField(rs.getString(i), i - 1);
			}
			boolean flag = lineSender.send(oneLine);
			if (flag) {
				monitor.increaseSuccessLines();
			} else {
				monitor.increaseFailedLines();
			}
		}
		lineSender.flush();
		rs.close();
		stmt.close();
	}

	private int printMetaDataInfoAndGetColumnCount(ResultSet rs)
			throws SQLException {
		ResultSetMetaData rsmd = rs.getMetaData();
		int columnCount = rsmd.getColumnCount();
		StringBuilder sb = new StringBuilder(500);
		sb.append("selected column names: \n");
		for (int i = 1; i <= columnCount; i++) {
			sb.append(rsmd.getColumnTypeName(i)).append(" ")
					.append(rsmd.getColumnName(i)).append(", ");
		}
		LOG.info(sb.substring(0, sb.length() - 2));
		return columnCount;
	}

	public static class Builder {
		private String path;
		private String username;
		private String password;
		private String sql;

		public Builder(String path) {
			this.path = path;
		}

		public Builder username(String username) {
			this.username = username;
			return this;
		}

		public Builder password(String password) {
			this.password = password;
			return this;
		}

		public Builder sql(String sql) {
			this.sql = sql;
			return this;
		}

		public HiveJdbcClient build() {
			return new HiveJdbcClient(this);
		}
	}

	private HiveJdbcClient(Builder b) {
		path = b.path;
		username = b.username;
		password = b.password;
		sql = b.sql;
	}
}
