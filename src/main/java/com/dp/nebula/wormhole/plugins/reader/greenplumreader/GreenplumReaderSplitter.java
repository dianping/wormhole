package com.dp.nebula.wormhole.plugins.reader.greenplumreader;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.StringTokenizer;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.dp.nebula.common.utils.DateHelper;
import com.dp.nebula.wormhole.common.AbstractSplitter;
import com.dp.nebula.wormhole.common.JobStatus;
import com.dp.nebula.wormhole.common.WormholeException;
import com.dp.nebula.wormhole.common.interfaces.IParam;

public class GreenplumReaderSplitter extends AbstractSplitter{	
	private int errorCodeAdd;
	
	private Log logger = LogFactory.getLog(GreenplumReaderSplitter.class);
		
	private String partitionName;
	
	private String partitionValue;
	
	private String tableName;
	
	private String columns;
	
	private String where;
				
	private int concurrency;
	
	private boolean needSplit;
	
	private String sql;
	
	private static final String COPY_SQL = "copy (%s) to stdout WITH DELIMITER E'\t' ";
	
	private static final String SIMPLE_SQL_WITH_WHERE_PATTEN = "select %s from %s where %s";
	
	private static final String SIMPLE_SQL_WITHOUT_WHERE_PATTEN = "select %s from %s";
	
	private static final String SQL_WITH_WHERE_PATTEN = "select %s from %s where %s and %s = \'%s\'";
	
	private static final String SQL_WITHOUT_WHERE_PATTEN = "select %s from %s where %s = \'%s\'";
		
	@Override
	public void init(IParam jobParams){
		super.init(jobParams);
		partitionName = param.getValue(ParamKey.partitionName, "");
		partitionValue = param.getValue(ParamKey.partitionValue, "");
		tableName = param.getValue(ParamKey.tableName, "");
		columns = param.getValue(ParamKey.columns, "*");
		where = param.getValue(ParamKey.where,"");
		concurrency = param.getIntValue(ParamKey.concurrency,1);
		needSplit = param.getBooleanValue(ParamKey.needSplit,false);
		sql = param.getValue(ParamKey.sql,"");
		errorCodeAdd = JobStatus.PLUGIN_BASE*GreenplumReader.PLUGIN_NO;
	}

	@Override
	public List<IParam> split() {
		if(!needSplit) {
			List<IParam> paramList = new ArrayList<IParam>() ;
			IParam paramNoSplitted = param.clone();
			if(sql.isEmpty()&&!tableName.isEmpty()&&!columns.isEmpty()){
				String noSplitSql = "";
				if(!where.isEmpty()) {
					noSplitSql = String.format(SIMPLE_SQL_WITH_WHERE_PATTEN, columns, tableName, where);
				} else {
					noSplitSql = String.format(SIMPLE_SQL_WITHOUT_WHERE_PATTEN, columns, tableName);
				}
				noSplitSql = String.format(COPY_SQL,noSplitSql);
				paramNoSplitted.putValue(ParamKey.sql, noSplitSql);
				paramList.add(paramNoSplitted);

			} else {
				sql = String.format(COPY_SQL,sql);
				paramNoSplitted.putValue(ParamKey.sql, sql);
				paramList.add(paramNoSplitted);
			}
			return paramList;
		}
		if(partitionName.isEmpty() || partitionValue.isEmpty() || tableName.isEmpty()){
			return super.split();
		}
		logger.info("Greenplum reader start to split");
		List<IParam> paramList = new ArrayList<IParam>() ;
		List<String> partitionList = getPartitionValueList();
		StringBuilder []sqlArray = new StringBuilder[concurrency];
		for(int i = 0; i < partitionList.size(); i++){
			String sqlSplitted = null;
			if(!where.isEmpty()) {
				sqlSplitted = String.format(SQL_WITH_WHERE_PATTEN, columns, tableName, where, partitionName,partitionList.get(i));
			} else {
				sqlSplitted = String.format(SQL_WITHOUT_WHERE_PATTEN, columns, tableName, partitionName,partitionList.get(i));
			}
			sqlSplitted = String.format(COPY_SQL,sqlSplitted);
			int index = (int) (i%concurrency);
			if(sqlArray[index] == null){
				sqlArray[index] =  new StringBuilder();
			}
			sqlArray[index].append(sqlSplitted).append(";") ;
		}
		for(int j = 0; j < concurrency; j++){
			if(sqlArray[j] == null) {
				continue;
			}
			IParam paramSplitted = param.clone();
			paramSplitted.putValue(ParamKey.sql, sqlArray[j].toString());
			logger.info(sqlArray[j].toString());
			paramList.add(paramSplitted);
		}
		logger.info("Greenplum reader is splitted successfully");
		return paramList;
	}
	
	private List<String> getPartitionValueList(){
		List<String> result = new ArrayList<String>(); 
		StringTokenizer tokens = new StringTokenizer(partitionValue);
		while (tokens.hasMoreTokens()) {
			String range = tokens.nextToken();
			String[] items = range.split("~");
			if (items.length == 1) {
				result.add(items[0]);
				continue;
			} else if (items.length == 2) {
				Date startDate = null, endDate = null;
				int startInt = 0, endInt = 0;
				startDate = DateHelper.parse(items[0],DateHelper.DATE_FORMAT_PATTERN_YEAR_MONTH_DAY,null);
				endDate = DateHelper.parse(items[1],DateHelper.DATE_FORMAT_PATTERN_YEAR_MONTH_DAY,null);
				if (startDate != null && endDate != null) {
					Date day = (Date) startDate.clone();
					while (day.before(endDate) || day.equals(endDate)) {
						result.add(DateHelper.format(day,DateHelper.DATE_FORMAT_PATTERN_YEAR_MONTH_DAY));
						day = DateHelper.changeDays(day, 1);
					}
					continue;
				} else if (startDate == null || endDate == null) {
					try{
						startInt = Integer.parseInt(items[0]);
						endInt = Integer.parseInt(items[1]);
					} catch (Exception e) {
						throw new WormholeException(e,JobStatus.READ_FAILED.getStatus()+errorCodeAdd);	
					}
					while (startInt <= endInt) {
						result.add(String.valueOf(startInt));
						startInt ++;
					}
					continue;
				} 	
			}
			logger.error("Greenplum partition value format error.");
			throw new WormholeException("Greenplum partition value format error.",JobStatus.READ_FAILED.getStatus()+errorCodeAdd);	
		}
		return result;
	}
}
