package com.dp.nebula.wormhole.plugins.reader.sqlserverreader;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.nebula.wormhole.common.JobStatus;
import com.dp.nebula.wormhole.common.WormholeException;
import com.dp.nebula.wormhole.common.interfaces.IParam;
import com.dp.nebula.wormhole.common.interfaces.IReaderPeriphery;
import com.dp.nebula.wormhole.common.interfaces.ISourceCounter;
import com.dp.nebula.wormhole.common.interfaces.ITargetCounter;
import com.dp.nebula.wormhole.plugins.common.DBSource;
import com.dp.nebula.wormhole.plugins.common.DBUtils;


public class SqlserverReaderPeriphery implements IReaderPeriphery{
	private Log logger = LogFactory.getLog(SqlserverReaderPeriphery.class);
	
	private Connection conn;

	/* below for job-xml variant */
	private String username;

	private String password;

	private String ip;

	private String port = "1433";

	private String dbname;

	private int concurrency;

	private String sqlserverParams;
	
	private String preSql;
	
	private String countSql;


	private void getConnection(IParam param) {
		/* for database connection */
		this.username = param.getValue(ParamKey.username, "");
		this.password = param.getValue(ParamKey.password, "");
		this.ip = param.getValue(ParamKey.ip,"");
		this.port = param.getValue(ParamKey.port, this.port);
		this.dbname = param.getValue(ParamKey.dbname,"");
		this.sqlserverParams = param.getValue(ParamKey.sqlServerParams,"");
		this.preSql =  param.getValue(ParamKey.preCheck, "").trim();
		/* for connection session */
		this.concurrency = param.getIntValue(ParamKey.concurrency, 1);
		this.countSql = param.getValue(ParamKey.countSql, "");
		Properties p = createProperties(param.getValue(ParamKey.url, null));
		try {
			DBSource.register(SqlserverReader.class, this.ip, this.port, this.dbname, p);
			conn = DBSource.getConnection(SqlserverReader.class, ip, port, dbname);
		} catch (Exception e) {
			throw new WormholeException(e, JobStatus.READ_CONNECTION_FAILED.getStatus() + SqlserverReader.ERROR_CODE_ADD);
		}
	}

	@Override
	public void prepare(IParam param, ISourceCounter counter) {
		getConnection(param);
		if(!preSql.isEmpty()) {
			try {
				DBUtils.dbPreCheck(preSql, conn);
			} catch (WormholeException e1) {
				e1.setStatusCode(e1.getStatusCode() + SqlserverReader.ERROR_CODE_ADD);
				throw e1;
			}
		}
		if(countSql.isEmpty()) {
			logger.info("Count sql is empty.");
			return;
		}
		ResultSet rs = null;
		try {
			logger.info("Count sql:" + countSql);
			rs = DBUtils.query(conn, countSql);
			rs.next();
			int size = rs.getInt(1);
			if(counter != null) {
				counter.setSourceLines(size);
			}
			logger.info("Source data size: " + size + " lines.");
		} catch (Exception e) {
			logger.error("Cannot get result set size!" ,e);
		}finally {
			if (null != rs) {
			    try {
					DBUtils.closeResultSet(rs);
				} catch (SQLException e) {
					logger.error("SqlserverReader close resultset error ");
					throw new WormholeException(e,JobStatus.READ_FAILED.getStatus()+SqlserverReader.ERROR_CODE_ADD);	
				}
            }
        	try {
    			conn.close();
    		} catch (SQLException e) {
    			logger.error("Cannot close connection " + e.getMessage(),e);
    		}
		}
	
	}
	
	@Override
	public void doPost(IParam param, ITargetCounter counter) {
		
	}

	private Properties createProperties(String url) {
		Properties p = new Properties();
		if(url == null) {
			url = String.format("jdbc:sqlserver://%s:%s;DatabaseName=%s;%s", this.ip,this.port,this.dbname,this.sqlserverParams);
		}
		p.setProperty("driverClassName", "com.microsoft.sqlserver.jdbc.SQLServerDriver");
		p.setProperty("url", url);
		p.setProperty("username", username);
		p.setProperty("password", password);
		p.setProperty("maxActive", String.valueOf(concurrency + 2));
		p.setProperty("initialSize", String.valueOf(concurrency + 2));
		p.setProperty("maxIdle", "1");
		p.setProperty("maxWait", "1000");
		p.setProperty("defaultReadOnly", "true");

		logger.debug(String.format("SqlserverReader try connection: %s .", url));
		return p;
	}
}
