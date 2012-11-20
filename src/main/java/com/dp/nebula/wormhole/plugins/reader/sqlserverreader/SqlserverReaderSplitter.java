package com.dp.nebula.wormhole.plugins.reader.sqlserverreader;

import java.sql.Connection;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.nebula.wormhole.common.AbstractSplitter;
import com.dp.nebula.wormhole.common.JobStatus;
import com.dp.nebula.wormhole.common.WormholeException;
import com.dp.nebula.wormhole.common.interfaces.IParam;
import com.dp.nebula.wormhole.plugins.common.DBSource;
import com.dp.nebula.wormhole.plugins.common.DBUtils;

public class SqlserverReaderSplitter  extends AbstractSplitter{
	private static final String SPLIT_SQL_WITH_WHERE_PATTEN = "select %s from %s where %s and %s > %d and %s <= %d";
	
	private static final String SPLIT_SQL_WITHOUT_WHERE_PATTEN = "select %s from %s where %s > %d and %s <= %d";
	
	private static final String SQL_WITH_WHERE_PATTEN = "select %s from %s where %s";
	
	private static final String SQL_WITHOUT_WHERE_PATTEN = "select %s from %s";
	
	private static final String RANGE_SQL_WITH_WHERE_PATTERN = "select min(%s), max(%s) from %s where %s";
	
	private static final String RANGE_SQL_WITHOUT_WHERE_PATTERN = "select min(%s), max(%s) from %s";
	
	private Log logger = LogFactory.getLog(SqlserverReaderSplitter.class);
		
	private String autoIncKey;
	
	private String tableName;
	
	private String columns;
	
	private String where;
	
	private int blockSize;
	
	private Connection conn;
	
	private String ip;

	private String port = "1433";

	private String dbname;
	
	private int concurrency;
	
	private boolean needSplit;
	
	private static final int DEFAULT_BLOCK_SIZE = 1000;
	
	@Override
	public void init(IParam jobParams){
		super.init(jobParams);
		autoIncKey = param.getValue(ParamKey.autoIncKey,"");
		tableName = param.getValue(ParamKey.tableName, "");
		columns = param.getValue(ParamKey.columns, "*");
		where = param.getValue(ParamKey.where,"");
		blockSize = param.getIntValue(ParamKey.blockSize, DEFAULT_BLOCK_SIZE);
		ip = param.getValue(ParamKey.ip,"");
		port = param.getValue(ParamKey.port, this.port);
		dbname = param.getValue(ParamKey.dbname,"");
		concurrency = param.getIntValue(ParamKey.concurrency,1);
		needSplit = param.getBooleanValue(ParamKey.needSplit,true);
		if(!autoIncKey.isEmpty() || !tableName.isEmpty()){
			try {
				conn = DBSource.getConnection(SqlserverReader.class, ip, port, dbname);
			} catch (Exception e) {
				throw new WormholeException(e, JobStatus.READ_CONNECTION_FAILED.getStatus() + SqlserverReader.ERROR_CODE_ADD);
			}
		}
	}

	@Override
	public List<IParam> split() {
		List<IParam> paramList = new ArrayList<IParam>() ;
		if(autoIncKey.isEmpty() || tableName.isEmpty()){
			logger.warn("AutoIncKey or tableName is empty, sqlserverReader  cannot split!");
			needSplit = false;
		}
		if(!needSplit) {
			IParam paramNoSplitted = param.clone();
			if(!tableName.isEmpty()){
				String noSplitSql = "";
				if(!where.isEmpty()) {
					noSplitSql = String.format(SQL_WITH_WHERE_PATTEN, columns, tableName, where);
				} else {
					noSplitSql = String.format(SQL_WITHOUT_WHERE_PATTEN, columns, tableName);
				}
				paramNoSplitted.putValue(ParamKey.sql, noSplitSql);
			} 
			paramList.add(paramNoSplitted);
			return paramList;
		}
		
		logger.info("Sqlserver reader start to split");

		String rangeSql = "";
		if(!where.isEmpty()) {
			rangeSql = String.format(RANGE_SQL_WITH_WHERE_PATTERN, autoIncKey,autoIncKey,tableName,where);
		} else {
			rangeSql = String.format(RANGE_SQL_WITHOUT_WHERE_PATTERN, autoIncKey,autoIncKey,tableName);
		}
		long min=0,max=0;
		
		try {
			logger.debug(rangeSql);
			ResultSet rs = DBUtils.query(conn, rangeSql);
			rs.next();
			min = rs.getInt(1);
			max = rs.getInt(2);
			rs.close();
		} catch (Exception e) {
			logger.error(e.getMessage(),e);
			throw new WormholeException(e,JobStatus.READ_FAILED.getStatus()+SqlserverReader.ERROR_CODE_ADD);
		}
		long start = min - 1;
		long end = min - 1 + blockSize;
		StringBuilder []sqlArray = new StringBuilder[concurrency];
		for(long i = 0; i <= (max-min)/blockSize; i++){
			String sqlSplitted = null;
			if(!where.isEmpty()) {
				sqlSplitted = String.format(SPLIT_SQL_WITH_WHERE_PATTEN, columns, tableName, where, autoIncKey, start, autoIncKey,end);
			} else {
				sqlSplitted = String.format(SPLIT_SQL_WITHOUT_WHERE_PATTEN, columns, tableName, autoIncKey, start, autoIncKey,end);
			}
			int index = (int) (i%concurrency);
			if(sqlArray[index] == null){
				sqlArray[index] =  new StringBuilder();
			}
			sqlArray[index].append(sqlSplitted).append(";") ;
			start += blockSize;
			end += blockSize;
			if(end > max) {
				end = max;
			}
		}
		for(int j = 0; j < concurrency; j++){
			if(sqlArray[j] == null) {
				continue;
			}
			IParam paramSplitted = param.clone();
			paramSplitted.putValue(ParamKey.sql, sqlArray[j].toString());
			paramList.add(paramSplitted);
		}
		logger.info("sqlServer reader is splitted successfully");
		return paramList;
	}
}