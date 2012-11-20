package com.dp.nebula.wormhole.plugins.reader.hivereader;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;

import com.dp.nebula.wormhole.common.AbstractPlugin;
import com.dp.nebula.wormhole.common.JobStatus;
import com.dp.nebula.wormhole.common.WormholeException;
import com.dp.nebula.wormhole.common.interfaces.ILine;
import com.dp.nebula.wormhole.common.interfaces.ILineSender;
import com.dp.nebula.wormhole.common.interfaces.IReader;

public class HiveReader extends AbstractPlugin implements IReader {
	private static final Logger logger = Logger.getLogger(HiveReader.class);
	private static final String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";
	
	private String path = "jdbc:hive://10.1.1.161:10000/default";
	private String username = "";
	private String password = "";
	private String sql = "";
	
	private Connection conn;
	
	@Override
	public void init() {
		this.path = getParam().getValue(ParamKey.path, this.path);
		this.username = getParam().getValue(ParamKey.username, this.username);
		this.password = getParam().getValue(ParamKey.password, this.password);	
		this.sql = getParam().getValue(ParamKey.sql, this.sql).trim();
	}
	
	@Override
	public void connection() {
		try {
			Class.forName(driverName);
			conn = DriverManager.getConnection(this.path, this.username, this.password);
		} catch (ClassNotFoundException ce) {
			logger.error("hive driver class not found, " + ce.getMessage());
			throw new WormholeException(ce, JobStatus.READ_CONNECTION_FAILED.getStatus());
		} catch (SQLException se) {
			logger.error("hive get connection error, " + se.getMessage());
			throw new WormholeException(se, JobStatus.READ_CONNECTION_FAILED.getStatus());
		}
	}

	@Override
	public void read(ILineSender lineSender) {
		try {
			Statement stmt = conn.createStatement();
			
			logger.info("hive execute sql:" + this.sql);
			ResultSet rs = stmt.executeQuery(this.sql);
			ResultSetMetaData rsmd = rs.getMetaData();
			int columnCount = rsmd.getColumnCount();
			StringBuilder sb = new StringBuilder(500);
			sb.append("selected column names: \n");
			for (int i = 1; i <= columnCount; i++) {
				sb.append(rsmd.getColumnTypeName(i)).append(" ")
					.append(rsmd.getColumnName(i)).append(", ");
			}
			logger.info(sb.substring(0, sb.length() - 2));
			
			while(rs.next()){
				ILine oneLine = lineSender.createNewLine();
				for (int i = 1; i <= columnCount; i++) {
					oneLine.addField(rs.getString(i), i - 1);
				}
				boolean flag = lineSender.send(oneLine);
				if (flag)
					getMonitor().increaseSuccessLines();
				else
					getMonitor().increaseFailedLines();
				
				//logger.warn(oneLine.toString('\t'));
			}
			lineSender.flush();
			
			rs.close();
			stmt.close();
		} catch (Exception e) {
			logger.error("something wrong with hive reader " + e.getMessage());
			throw new WormholeException(e, JobStatus.READ_DATA_EXCEPTION.getStatus());
		}
	}
	
	@Override
	public void finish() {
		if (conn != null){
			try {
				conn.close();
			} catch (SQLException e) {
				logger.error("close connection error, " + e.getMessage());
				throw new WormholeException(e, JobStatus.READ_FAILED.getStatus());
			}
		}
	}
}
