package com.dp.nebula.wormhole.plugins.writer.greenplumwriter;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.Properties;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.nebula.wormhole.common.AbstractPlugin;
import com.dp.nebula.wormhole.common.WormholeException;
import com.dp.nebula.wormhole.common.interfaces.IParam;
import com.dp.nebula.wormhole.common.interfaces.ISourceCounter;
import com.dp.nebula.wormhole.common.interfaces.ITargetCounter;
import com.dp.nebula.wormhole.common.interfaces.IWriterPeriphery;
import com.dp.nebula.wormhole.plugins.common.DBSource;
import com.dp.nebula.wormhole.plugins.common.DBUtils;
import com.dp.nebula.wormhole.common.JobStatus;

public class GreenplumWriterPeriphery implements IWriterPeriphery{
	
	private Log logger = LogFactory.getLog(GreenplumWriterPeriphery.class);
	
	private Connection conn;
	
	private String username;
	
	private String password;

	private String ip;

	private String port = "5432";

	private String dbname;

	private String gpParams;
	
	private String encode;
	
	private String rollback;
	
	private String pre;
	
	private String post;
	
	private int concurrency = 1;
	
	private String countSql;
	
	private String writerID;
	
	private int errorCodeAdd;
	
	@Override
	public void prepare(IParam param, ISourceCounter counter) {
		register(param);
		if(pre.isEmpty()){
			return;
		}
		try{
			conn = DBSource.getConnection(GreenplumWriter.class, ip, writerID, dbname);
			String[] sqlArray = pre.split(";");
			for(String sql:sqlArray){
				sql = sql.trim();
				if(sql.isEmpty()) {
					continue;
				}
				DBUtils.update(conn, sql);
			}
			conn.close();
		}catch (Exception e) {
			logger.error(writerID + ": Greenplum writer prepare failed. ");
			throw new WormholeException(e,JobStatus.PRE_WRITE_FAILED.getStatus()+errorCodeAdd,writerID);
		}
	}
	
	@Override
	public void rollback(IParam param) {
		if(rollback.isEmpty()){
			logger.error(writerID + ": Fail to roll back because rollback sql is empty!");
			throw new WormholeException("Fail to roll back because rollback sql is empty!",JobStatus.ROLL_BACK_FAILED.getStatus()+errorCodeAdd,writerID);
		}
		try{
			conn = DBSource.getConnection(GreenplumWriter.class, ip, writerID, dbname);
			String[] sqlArray = rollback.split(";");
			for(String sql:sqlArray){
				sql = sql.trim();
				if(sql.isEmpty()) {
					continue;
				}
				DBUtils.update(conn, sql);
			}
			conn.close();
		} catch (Exception e) {
			logger.error(writerID + ": Greenplum writer roll back failed. ");
			throw new WormholeException(e,JobStatus.ROLL_BACK_FAILED.getStatus()+errorCodeAdd,writerID);
		}
	}

	@Override
	public void doPost(IParam param, ITargetCounter counter,int faildSize) {
		if(!countSql.isEmpty()){
			try{
				conn = DBSource.getConnection(GreenplumWriter.class, ip, writerID, dbname);
				ResultSet rs = DBUtils.query(conn, countSql);
				rs.next();
				int lines = rs.getInt(1);
				counter.setTargetLines(writerID, lines);
				rs.close();
				conn.close();
			}catch (Exception e) {
				logger.error(writerID + ": Greenplum writer count line number failed. ",e);
			}
		}
		if(!post.isEmpty()){
			try{
				conn = DBSource.getConnection(GreenplumWriter.class, ip, writerID, dbname);
				String[] sqlArray = post.split(";");
				for(String sql:sqlArray){
					sql = sql.trim();
					if(sql.isEmpty()) {
						continue;
					}
					DBUtils.update(conn, sql);
				}
				conn.close();
				conn.close();
			}catch (Exception e) {
				logger.error(writerID + ": Greenplum writer dopost failed. ");
				throw new WormholeException(e,JobStatus.POST_WRITE_FAILED.getStatus()+errorCodeAdd,writerID);
			}
		}
	}
	
	private void init(IParam param){
		/* for database connection */
		this.username 	  = param.getValue(ParamKey.username,"");
		this.password 	  = param.getValue(ParamKey.password,"");
		this.ip 		  = param.getValue(ParamKey.ip,"");
		this.port 		  = param.getValue(ParamKey.port, this.port);
		this.dbname 	  = param.getValue(ParamKey.dbname,"");
		this.encode       = param.getValue(ParamKey.encoding, "");
		this.gpParams 	  = param.getValue(ParamKey.greenplumParams,"");
		this.rollback     = param.getValue(ParamKey.rollback,"");
		this.pre          = param.getValue(ParamKey.pre,"");
		this.post         = param.getValue(ParamKey.post,"");
		this.countSql     = param.getValue(ParamKey.countSql,"");
		this.writerID	  = param.getValue(AbstractPlugin.PLUGINID, "");
		int priority      = param.getIntValue(ParamKey.priority, 0);
		this.errorCodeAdd = GreenplumWriter.PLUGIN_NO*JobStatus.PLUGIN_BASE + priority*JobStatus.WRITER_BASE;

	}
	
	private void register(IParam param) {
		init(param);
		/* for connection session */
		Properties p = createProperties();
		try {
			DBSource.register(GreenplumWriter.class, this.ip, this.writerID, this.dbname, p);
		} catch (Exception e) {
			throw new WormholeException(e, JobStatus.WRITE_CONNECTION_FAILED.getStatus() + errorCodeAdd);
		}
	}
		
	private Properties createProperties() {
		Properties p = new Properties();
		
		String encodeDetail = "";
		
		if(!StringUtils.isBlank(this.encode)){
			encodeDetail = "useUnicode=true&characterEncoding="	+ this.encode + "&";
		}
//		String url = "jdbc:postgresql://" + this.ip + ":" + this.port + "/"
//				+ this.dbname + "?" + encodeDetail 
//				+ "yearIsDateType=false&zeroDateTimeBehavior=convertToNull&loglevel=2"
//				+ "&defaultFetchSize=" + String.valueOf(Integer.MIN_VALUE);
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
		p.setProperty("maxActive", String.valueOf(concurrency + 2));
		//don't set initialSize, otherwise accessToUnderlyingConnectionAllowed will not be set successfully
		//p.setProperty("initialSize", String.valueOf(concurrency + 2));
		p.setProperty("maxIdle", "1");
		p.setProperty("maxWait", "1000");
		p.setProperty("defaultReadOnly", "false");
		p.setProperty("testOnBorrow", "true");
		p.setProperty("loglevel", "2");

		logger.debug(String.format(writerID + ": Greenplum try connection: %s .", url));
		return p;
	}
}
