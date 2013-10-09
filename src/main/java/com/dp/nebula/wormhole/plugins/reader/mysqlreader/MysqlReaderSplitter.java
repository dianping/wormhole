package com.dp.nebula.wormhole.plugins.reader.mysqlreader;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.nebula.wormhole.common.AbstractSplitter;
import com.dp.nebula.wormhole.common.JobStatus;
import com.dp.nebula.wormhole.common.WormholeException;
import com.dp.nebula.wormhole.common.interfaces.IParam;


public class MysqlReaderSplitter extends AbstractSplitter{

	private static final String SQL_PATTEN = "%s limit %d, %d";
	
	private Log logger = LogFactory.getLog(MysqlReaderSplitter.class);
	
	private String sql;
	
	private int blockSize;
	
	private int concurrency;
	
	private String tableName;
	
	private String columns;
	
	private String where;
	
	private static final int DEFAULT_BLOCK_SIZE = 1000;
	
	private static final String SQL_WITH_WHERE_PATTEN = "select %s from %s where %s";
	
	private static final String SQL_WITHOUT_WHERE_PATTEN = "select %s from %s";
	
	@Override
	public void init(IParam jobParams){
		super.init(jobParams);
		sql = param.getValue(ParamKey.sql, "");
		blockSize = param.getIntValue(ParamKey.blockSize, DEFAULT_BLOCK_SIZE);
		concurrency = param.getIntValue(ParamKey.concurrency,1);
		tableName = param.getValue(ParamKey.tableName, "");
		columns = param.getValue(ParamKey.columns, "");
		where = param.getValue(ParamKey.where, "");
	}

	@Override
	public List<IParam> split() {
		List<IParam> paramList = new ArrayList<IParam>() ;
		if(sql.isEmpty()) {
			if(tableName.isEmpty()||columns.isEmpty()) {
				logger.error("Mysql reader sql is empty");
				throw new WormholeException("Mysql reader sql is empty",JobStatus.CONF_FAILED.getStatus()+MysqlReader.ERROR_CODE_ADD);
			}
			if(!where.isEmpty()) {
				sql = String.format(SQL_WITH_WHERE_PATTEN, columns, tableName, where);
			} else {
				sql = String.format(SQL_WITHOUT_WHERE_PATTEN, columns, tableName);
			}
		}
		if(!sql.isEmpty()){
			long size = param.getLongValue(MysqlReaderPeriphery.DATA_AMOUNT_KEY,-1);
			if (size == -1){
				paramList.add(param);
				logger.warn("Cannot get data amount for mysql reader");
				return paramList;
			}
			int amount = 0;
			StringBuilder []sqlArray = new StringBuilder[concurrency];
			for(long i = 0; i <= size/blockSize; i++){
				String sqlSplitted = String.format(SQL_PATTEN, sql, amount, blockSize);
				int index = (int) (i%concurrency);
				if(sqlArray[index] == null){
					sqlArray[index] =  new StringBuilder();
				}
				sqlArray[index].append(sqlSplitted).append(";") ;
				amount += blockSize;
			}
			for(int j = 0; j < concurrency; j++){
				if(sqlArray[j] == null) {
					continue;
				}
				IParam paramSplitted = param.clone();
				paramSplitted.putValue(ParamKey.sql, sqlArray[j].toString());
				paramList.add(paramSplitted);
			}
		}
		return paramList;
	}
}
