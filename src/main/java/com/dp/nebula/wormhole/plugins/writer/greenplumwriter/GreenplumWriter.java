package com.dp.nebula.wormhole.plugins.writer.greenplumwriter;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.dbcp.DelegatingConnection;
import org.apache.log4j.Logger;
import org.postgresql.PGConnection;
import org.postgresql.copy.PGCopyOutputStream;
import org.postgresql.util.PSQLException;

import com.dp.nebula.wormhole.common.AbstractPlugin;
import com.dp.nebula.wormhole.common.JobStatus;
import com.dp.nebula.wormhole.common.WormholeException;
import com.dp.nebula.wormhole.common.interfaces.ILine;
import com.dp.nebula.wormhole.common.interfaces.ILineReceiver;
import com.dp.nebula.wormhole.common.interfaces.IWriter;
import com.dp.nebula.wormhole.plugins.common.DBSource;
import com.dp.nebula.wormhole.plugins.common.ErrorCodeUtils;

public class GreenplumWriter  extends AbstractPlugin implements IWriter {
	static final int PLUGIN_NO = 2;
	private int errorCodeAdd;
	private static Map<String, String> encodingMaps = null;
	static {
		encodingMaps = new HashMap<String, String>();
		encodingMaps.put("utf-8", "UTF8");
	}
	
	private Connection conn;

	private String ip = "";
	
	private String dbname = null;

	private String sql;
	
	private String table;
	
	private String columns;

	private String encoding = "UTF8";

	//private double limit = 0;

	//private int lineCounter = 0;
    //0x0 is invalid in greenplum 
//    private static char[] replaceChars = {0,' '};

	private int sucLineCounter = 0;
	
	private int failedLineCounter = 0;
	
	private String logErrorTable;
	
	private int failedLinesThreshold;

    /* 列分隔符 */
    private static final char SEP = '\t';
    /* 行分隔符 */
    // In windows this may be '\r\n'
    private static final char BREAK = '\n';
    
    private ILine line = null;
    /* 从line中获取一行数据暂存数组*/
    private byte buffer[] = null;
    
//    private int length = 1024 * 1024 * 8;
    		
	private String writerID;
    
    //private StringBuilder lineBuilder = new StringBuilder(len);
    
	private Logger logger = Logger.getLogger(GreenplumWriter.class);
	
	private static final String SQL_PATTERN = "COPY %s %s FROM STDIN WITH DELIMITER E'%c' ";
	
	private static final String SQL_ERROR_LOG_PATTERN = "%s LOG ERRORS INTO %s";
	
	private static final String SQL_REJECT_LIMIT_PATTERN = "%s SEGMENT REJECT LIMIT %d ROWS";
	
//	private final String zeroDateTimeRound = "0001-01-01 00:00:00.0";
	
	private String[] addFields = null;
	
	@Override
	public void init() {
		this.ip = getParam().getValue(ParamKey.ip,"");
		this.dbname = getParam().getValue(ParamKey.dbname,"");
		this.table = getParam().getValue(ParamKey.table,"");
		this.columns = getParam().getValue(ParamKey.columns,"");
		this.encoding = getParam().getValue(ParamKey.encoding, "UTF8").toLowerCase();
		this.logErrorTable = getParam().getValue(ParamKey.logErrorTable, "");
		this.failedLinesThreshold = getParam().getIntValue(ParamKey.failedLinesThreshold, 0);
		this.writerID	 = getParam().getValue(AbstractPlugin.PLUGINID, "");
		analyzeColumns();
		if (encodingMaps.containsKey(this.encoding)) {
			this.encoding = encodingMaps.get(this.encoding);
		}
		if(!columns.isEmpty()) {
			columns = "(" + columns + ")";
		}
		sql = String.format(SQL_PATTERN, table, columns,SEP);
		if(!logErrorTable.isEmpty()) {
			sql = String.format(SQL_ERROR_LOG_PATTERN,sql,logErrorTable);
		}
		if(failedLinesThreshold > 1) {
			sql = String.format(SQL_REJECT_LIMIT_PATTERN,sql,failedLinesThreshold);
		}
		int priority = getParam().getIntValue(ParamKey.priority, 0);
		errorCodeAdd = PLUGIN_NO*JobStatus.PLUGIN_BASE + priority*JobStatus.WRITER_BASE;
	}

	@Override
	public void connection() {
		try {
			conn = DBSource.getConnection(GreenplumWriter.class, ip, writerID, dbname);
		} catch (Exception e) {
			throw new WormholeException(e, JobStatus.WRITE_CONNECTION_FAILED.getStatus() + errorCodeAdd);
		}
	}
	
	@Override
	public void write(ILineReceiver receiver) {
		logger.info(writerID + ": insert SQL - " + sql);
		PGCopyOutputStream outputStream = null;
		try {
			outputStream = new PGCopyOutputStream((PGConnection) ((DelegatingConnection) conn).getInnermostDelegate(), sql); 
			while(true){
				if(fetchLine(receiver, outputStream) == -1){
					break;
				}
			}
			outputStream.flushCopy();
			getMonitor().increaseSuccessLine(sucLineCounter);
			getMonitor().increaseFailedLines(failedLineCounter);
		} catch (Exception e) {
			logger.error(writerID + ": write to greenplum failed.");
			throw throwException(e);	
		} 
		finally{
			if(outputStream != null) {
				try {
					outputStream.close();
				} catch (IOException e) {
					throw throwException(e);	
				}
			}
			this.logger.info(writerID + ": write to greenplum ends .");
			try {
				if(conn != null) {
					conn.close();
				}
			} catch (Exception e) {
				logger.error(writerID + ": close connection failed.",e);
			}
		}
	}
	
	private WormholeException throwException(Exception e) {
		WormholeException ex = new WormholeException(e,JobStatus.WRITE_FAILED.getStatus(),writerID);
		if(e instanceof WormholeException) {
			ex = (WormholeException) e;
		}
		else if(e instanceof PSQLException) {
			ErrorCodeUtils.psqlWriterWrapper((PSQLException)e,ex);
		} 
		else if(e instanceof IOException && e.getCause() != null 
				&& e.getCause() instanceof PSQLException) {
			ErrorCodeUtils.psqlWriterWrapper((PSQLException)(e.getCause()),ex);
		}
		ex.setStatusCode(ex.getStatusCode() + errorCodeAdd);
		return ex;
	}

	public String getEncoding() {
		return encoding;
	}

	@Override
	public void commit() {
	}
	
	private void analyzeColumns(){
		if(columns.isEmpty()) {
			return;
		}
		String tmpColumns = columns;
		String [] tmpColumnsArr = tmpColumns.split(",");
		addFields = new String[tmpColumnsArr.length];
		columns = "";
		int i = 0;
		for(String field:tmpColumnsArr){
			int index = field.indexOf("=");
			if(index != -1) {
				if(index + 1 != field.length()) {
					String columnValue = field.substring(index+1);
					addFields[i] = columnValue;
				}
				else {
					addFields[i] = "";
				}
				field = field.substring(0,index).trim();
			} 
			if (columns.isEmpty()) {
				columns = field;
			}
			else {
				columns = columns + "," + field;
			}
			i++;	
		}
	}
	
	private String buildString(ILine line) {
		StringBuilder lineBuilder = new StringBuilder();
		String field;
	    int num = line.getFieldNum();
	    int len = 0;
	    if(addFields == null ) {
	    	len = num;
	    } else {
	    	len = addFields.length;
	    }
	    for (int i = 0,j = 0;i < len; i++) {
	    	if(addFields!=null && addFields[i] != null) {
	    		field = addFields[i];
	    	}
	    	else if(j < num){
	    		field = line.getField(j);
	    		j++;
	    	}
	    	else {
	    		logger.error(writerID + ": field number is less than column number.");
	    		throw new WormholeException("GreenplumWriter: Fields number is less than column number ",JobStatus.WRITE_FAILED.getStatus(),writerID);
	    	}
	        if (null != field) {
	        	StringBuilder sb = new StringBuilder();
	        	char[] characters = field.toCharArray();
	        	for(int k = 0; k < characters.length; k++){
	        		if(characters[k] == '\\'){
	        			sb.append("\\\\");
	        		}
	        		else if(characters[k] == '\r'){
	        			sb.append("\\r");
	        		}
	        		else if(characters[k] == '\n'){
	        			sb.append("\\n");
	        		}
	        		else if(characters[k] == SEP){
	        			sb.append("\\t");
	        		}
	        		else if(characters[k] == 0) {
	        			sb.append(" ");
	        		}
	        		else{
	        			sb.append(characters[k]);
	        		}
	        	}
	            lineBuilder.append(sb.toString());
	        } else {
	            lineBuilder.append("\\N");
	        }
	        if (i < len - 1) {
	        	lineBuilder.append(SEP);
	        } else {
	            lineBuilder.append(BREAK);
	        }
	    }
	    return lineBuilder.toString();
	}
	    
	private int fetchLine(ILineReceiver receiver, PGCopyOutputStream outputStream) throws UnsupportedEncodingException {
        boolean isSuccess = true;
		int ret = 0;
        //int currLen;
        
        /* 本次读数据的逻辑 */
        int lineLen;
        line = receiver.receive();
        /* line为空，表明数据已全部读完 */
        if (line == null) {
            return -1;
        }
        //this.buildString(line);
        this.buffer = buildString(line).getBytes(this.encoding);
        lineLen = this.buffer.length;
       
        try {
        	 outputStream.write(buffer);
//        	 if(lineLen > length) {
//             	logger.error(writerID + ": line is too long");
//             	isSuccess = false;
//             }
//        	 else {
//        		 outputStream.writeToCopy(buffer, 0, lineLen);
//        	 }
		} catch (Exception e) {
			logger.warn(writerID + ": Copy data failed for one Line.");
			if(!outputStream.isActive()) {
				logger.error(writerID + ": Copy data failed:" + line.toString('\t'),e);
				throw new WormholeException(e,JobStatus.WRITE_FAILED.getStatus(),writerID);
			}
         	isSuccess = false;
		}
		if(isSuccess) {
			this.sucLineCounter ++ ;
		} else {
			this.failedLineCounter ++ ;
		}
		ret += lineLen;
        return (ret);
	}

//	private String replaceChars(String old, char[] rchars) {
//		if (null == rchars) {
//			return old;
//		}
//		
//		int oldLen = old.length();
//		int rLen = rchars.length;
//		
//		StringBuilder sb = new StringBuilder(oldLen);
//		char[] oldArrays = old.toCharArray();
//		boolean found;
//		char c1;
//		
//		for (int i = 0; i < oldLen; i++) {
//			found = false;
//			c1 = oldArrays[i];
//			for (int j = 0; j < rLen; j += 2) {
//				if (c1 == rchars[j]) {
//					if (rchars[j + 1] != 0) {
//						sb.append(rchars[j + 1]);
//					}
//					found = true;
//				}
//			}
//			if (!found) {
//				sb.append(c1);
//			}
//		}	
//		return sb.toString();
//	}
}
