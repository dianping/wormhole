package com.dp.nebula.wormhole.plugins.reader.mysqlreader;

import static java.text.MessageFormat.format;

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

public class MysqlReaderPeriphery implements IReaderPeriphery{
		
	private Connection conn;

	/* below for job-xml variant */
	private String encode;

	private String username;

	private String password;

	private String ip;

	private String port = "3306";

	private String dbname;

	private int concurrency;

	private String mysqlParams;
	
	private String preSql;
	
	private String sql;
	
	private String countSql;
	
	private String autoIncKey;
	
	private String tableName;
	
	private boolean needSplit;
	
	private static final String SQL_COUNT_PATTERN = "select count(*) from ({0}) uni_​​alias_name_f" ;
	
	protected static final String DATA_AMOUNT_KEY = "dataamount"; 
	
	private Log logger = LogFactory.getLog(MysqlReaderPeriphery.class);

	private void init(IParam param) {
		/* for database connection */
		this.username = param.getValue(ParamKey.username, "");
		this.password = param.getValue(ParamKey.password, "");
		this.ip = param.getValue(ParamKey.ip,"");
		this.port = param.getValue(ParamKey.port, this.port);
		this.dbname = param.getValue(ParamKey.dbname,"");
		this.encode = param.getValue(ParamKey.encoding, "");
		this.mysqlParams = param.getValue(ParamKey.mysqlParams,"");
		this.autoIncKey = param.getValue(ParamKey.autoIncKey,"");
		this.tableName = param.getValue(ParamKey.tableName, "");
		this.sql = param.getValue(ParamKey.sql, "").trim();
		this.preSql =  param.getValue(ParamKey.preCheck, "").trim();
		/* for connection session */
		this.concurrency = param.getIntValue(ParamKey.concurrency, 1);
		this.countSql = param.getValue(ParamKey.countSql, "");
		needSplit = param.getBooleanValue(ParamKey.needSplit,true);
	}

	@Override
	public void prepare(IParam param, ISourceCounter counter) {
		init(param);
		Properties p = createProperties();
		try {
			DBSource.register(MysqlReader.class, this.ip, this.port, this.dbname, p);
			conn = DBSource.getConnection(MysqlReader.class, ip, port, dbname);
		} catch (Exception e) {
			throw new WormholeException(e, JobStatus.READ_CONNECTION_FAILED.getStatus() + MysqlReader.ERROR_CODE_ADD);
		}
		if(!preSql.isEmpty()) {
			try {
				DBUtils.dbPreCheck(preSql, conn);
			} catch (WormholeException e) {
				e.setStatusCode(e.getStatusCode() + MysqlReader.ERROR_CODE_ADD);
				throw e;
			}
		}
		//autoIncKey and tableName is not empty, than use key splitter, do not need count item number
		if(countSql.isEmpty()&&!sql.isEmpty() && needSplit && (autoIncKey.isEmpty() || tableName.isEmpty())) {
			countSql = format(SQL_COUNT_PATTERN, sql);
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
			param.putValue(DATA_AMOUNT_KEY, Integer.toString(size));
			counter.setSourceLines(size);
			logger.info("Source data size: " + size + " lines.");
		} catch (Exception e) {
			logger.error("Cannot get result set size!" );
			throw new WormholeException(e,JobStatus.READ_FAILED.getStatus()+MysqlReader.ERROR_CODE_ADD);
		}finally {
			if (null != rs) {
			    try {
					DBUtils.closeResultSet(rs);
				} catch (SQLException e) {
					logger.error("MysqlReader close resultset error " );
					throw new WormholeException(e,JobStatus.READ_FAILED.getStatus()+MysqlReader.ERROR_CODE_ADD);	
				}
            }
        	try {
    			conn.close();
    		} catch (SQLException e) {
    			logger.error("Cannot close connection ",e);
    		}
		}
	
	}
	
	@Override
	public void doPost(IParam param, ITargetCounter counter, int faildSize) {
		
	}

	private Properties createProperties() {
		Properties p = new Properties();
		
		String encodeDetail = "";
		
		if(!StringUtils.isBlank(this.encode)){
			encodeDetail = "useUnicode=true&characterEncoding="	+ this.encode + "&";
		}
		String url = "jdbc:mysql://" + this.ip + ":" + this.port + "/"
				+ this.dbname + "?" + encodeDetail 
				+ "yearIsDateType=false&zeroDateTimeBehavior=round"
				+ "&defaultFetchSize=" + String.valueOf(Integer.MIN_VALUE);
		logger.info(url);		
		if (!StringUtils.isBlank(this.mysqlParams)) {
			url = url + "&" + this.mysqlParams;
		}
		
		p.setProperty("driverClassName", "com.mysql.jdbc.Driver");
		p.setProperty("url", url);
		p.setProperty("username", username);
		p.setProperty("password", password);
		p.setProperty("maxActive", String.valueOf(concurrency + 2));
		p.setProperty("initialSize", String.valueOf(concurrency + 2));
		p.setProperty("maxIdle", "1");
		p.setProperty("maxWait", "1000");
		p.setProperty("defaultReadOnly", "true");
		p.setProperty("testOnBorrow", "true");
		p.setProperty("validationQuery", "select 1 from dual");

		logger.debug(String.format("MysqlReader try connection: %s .", url));
		return p;
	}
}
