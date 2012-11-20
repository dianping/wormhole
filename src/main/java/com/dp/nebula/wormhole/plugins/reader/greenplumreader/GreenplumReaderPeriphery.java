package com.dp.nebula.wormhole.plugins.reader.greenplumreader;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
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

public class GreenplumReaderPeriphery implements IReaderPeriphery{
		
	private Connection conn;

	/* below for job-xml variant */
	private String encode;

	private String username;

	private String password;

	private String ip;

	private String port = "5432";

	private String dbname;

	private String gpParams;
	
	private String preSql;
		
	private String countSql;
		
	protected static final String DATA_AMOUNT_KEY = "dataamount"; 
	
	private Log logger = LogFactory.getLog(GreenplumReaderPeriphery.class);


	private void getConnection(IParam param) {
		/* for database connection */
		this.username = param.getValue(ParamKey.username, "");
		this.password = param.getValue(ParamKey.password, "");
		this.ip = param.getValue(ParamKey.ip,"");
		this.port = param.getValue(ParamKey.port, this.port);
		this.dbname = param.getValue(ParamKey.dbname,"");
		this.encode = param.getValue(ParamKey.encoding, "");
		/* for connection session */
		this.preSql =  param.getValue(ParamKey.preCheck, "").trim();
		this.countSql = param.getValue(ParamKey.countSql, "");
		this.gpParams = param.getValue(ParamKey.greenplumParams, "");
		Properties p = createProperties();
		try {
			DBSource.register(GreenplumReader.class, this.ip, this.port, this.dbname, p);
			conn = DBSource.getConnection(GreenplumReader.class, ip, port, dbname);
		} catch (Exception e) {
			throw new WormholeException(e, JobStatus.READ_CONNECTION_FAILED.getStatus() + GreenplumReader.ERROR_CODE_ADD);
		}
	}

	@Override
	public void prepare(IParam param, ISourceCounter counter) {
		getConnection(param);
		if(!preSql.isEmpty()) {
			try {
				DBUtils.dbPreCheck(preSql, conn);
			} catch (WormholeException e1) {
				e1.setStatusCode(e1.getStatusCode() + GreenplumReader.ERROR_CODE_ADD);
				throw e1;
			}
		}
		if(countSql.isEmpty()) {
			logger.info("Count sql is empty.");
			return;
		}
		ResultSet rs = null;
		try {
			logger.info(countSql);
			rs = DBUtils.query(conn, countSql);
			rs.next();
			int size = rs.getInt(1);
			counter.setSourceLines(size);
			logger.info("Source data size: " + size + " lines.");
		} catch (Exception e) {
			logger.error("Cannot get result set size!" );
		}finally {
			if (null != rs) {
			    try {
					DBUtils.closeResultSet(rs);
				} catch (SQLException e) {
					logger.error("GreenplumReader close resultset error ");
					throw new WormholeException(e,JobStatus.READ_FAILED.getStatus()+GreenplumReader.ERROR_CODE_ADD);	
				}
            }
        	try {
    			conn.close();
    		} catch (SQLException e) {
    			logger.error("Cannot close connection " + e.getMessage());
    		}
		}
	
	}

	private Properties createProperties() {
		Properties p = new Properties();
		
		String encodeDetail = "";
		
		if(!StringUtils.isBlank(this.encode)){
			encodeDetail = "useUnicode=true&characterEncoding="	+ this.encode + "&";
		}
		String url = "jdbc:postgresql://" + this.ip + ":" + this.port + "/"
				+ this.dbname + "?" + encodeDetail 
				+ "yearIsDateType=false&zeroDateTimeBehavior=convertToNull"
				+ "&defaultFetchSize=" + String.valueOf(Integer.MIN_VALUE);
		
		if (!StringUtils.isBlank(this.gpParams)) {
			url = url + "&" + this.gpParams;
		}
		
		p.setProperty("driverClassName", "org.postgresql.Driver");
		p.setProperty("url", url);
		p.setProperty("username", username);
		p.setProperty("password", password);
		p.setProperty("maxActive", String.valueOf(3));
		p.setProperty("maxIdle", "1");
		p.setProperty("maxWait", "1000");
		p.setProperty("defaultReadOnly", "true");
		p.setProperty("testOnBorrow", "true");
		logger.debug(String.format("GreenplumReader try connection: %s .", url));
		return p;
	}

	@Override
	public void doPost(IParam param, ITargetCounter counter) {		
	}
}
