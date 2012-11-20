package com.dp.nebula.wormhole.plugins.writer.hdfswriter;

import java.io.IOException;
import java.net.URI;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Shell.ShellCommandExecutor;
import org.apache.log4j.Logger;

import com.dp.nebula.wormhole.common.interfaces.IParam;
import com.dp.nebula.wormhole.common.interfaces.ISourceCounter;
import com.dp.nebula.wormhole.common.interfaces.ITargetCounter;
import com.dp.nebula.wormhole.common.interfaces.IWriterPeriphery;
import com.dp.nebula.wormhole.plugins.common.DFSUtils;
import com.hadoop.compression.lzo.LzoIndexer;

import java.text.MessageFormat;

public class HdfsWriterPeriphery implements IWriterPeriphery {

	private static final Logger logger = Logger.getLogger(HdfsWriterPeriphery.class);
	private static final int HIVE_TABLE_ADD_PARTITION_PARAM_NUMBER = 2;
	
	private String dir = "";
	private String prefixname = "";
	private int concurrency = 1;
	private String codecClass = "";
	private String fileType = "TXT";
	private String suffix = "";
	private boolean lzoCompressed = false;
	private String hiveTableAddPartitionOrNot = "false";
	private String hiveTableAddPartitionCondition = "";
	
	private final String ADD_PARTITION_SQL = "alter table {0} add if not exists partition({1}) location ''{2}'';";
	
	private FileSystem fs;
	private LzoIndexer lzoIndexer;
	
	@Override
	public void rollback(IParam param) {
		deleteFilesOnHdfs(this.dir, this.prefixname, suffix);
	}

	@Override
	public void doPost(IParam param, ITargetCounter counter) {
		if (lzoCompressed){
			createLzoIndex(dir);
		}
		
		/* add hive table partition if necessary */
		hiveTableAddPartitionOrNot = param.getValue(ParamKey.hiveTableAddPartitionSwitch, "false").trim();
		if (hiveTableAddPartitionOrNot.equalsIgnoreCase("true")){
			hiveTableAddPartitionCondition = param.getValue(
					ParamKey.hiveTableAddPartitionCondition, this.hiveTableAddPartitionCondition);
			
			if (!StringUtils.isBlank(hiveTableAddPartitionCondition)){
				String[] hqlParam = StringUtils.split(hiveTableAddPartitionCondition, '@');
				if (HIVE_TABLE_ADD_PARTITION_PARAM_NUMBER == hqlParam.length){
					String parCondition = hqlParam[0].trim().replace('"', '\'');
					String uri = hqlParam[1].trim();
					//split dbname and tablename
					String[] parts = StringUtils.split(uri, '.');
					
					if (StringUtils.isBlank(parCondition) || StringUtils.isBlank(uri) 
							|| parts.length != 2 || StringUtils.isBlank(parts[0]) || StringUtils.isBlank(parts[1])){
						logger.error(ParamKey.hiveTableAddPartitionSwitch + 
								" param can not be parsed correctly, please check it again");
						return;
					}
						
					try {
						addHiveTablePartition(parts[0].trim(), parts[1].trim(), parCondition, dir);
					} catch (IOException e) {
						logger.error("add hive table partition failed:" + e.getMessage());
					}
				}
			}
		}
		return;
	}
	
	public void addHiveTablePartition(String dbName, String tableName, String partitionCondition, String location) throws IOException{
	    StringBuffer addParitionCommand = new StringBuffer();
	    
	    addParitionCommand.append("hive -e \"");
	    addParitionCommand.append("use " + dbName + ";");
	    addParitionCommand.append(MessageFormat.format(ADD_PARTITION_SQL, tableName, partitionCondition, location));
	    addParitionCommand.append("\"");
	    
	    logger.info(addParitionCommand.toString());
	    
	    String[] shellCmd = {"bash", "-c", addParitionCommand.toString()};
	    ShellCommandExecutor shexec = new ShellCommandExecutor(shellCmd);
	    shexec.execute();
	    int exitCode = shexec.getExitCode();
	    if (exitCode != 0) {
	      throw new IOException("process exited with exit code " + exitCode);
	    } else{
	    	logger.info("hive table add partion hql executed correctly:" + addParitionCommand.toString());
	    }
	}
	
//	private void createHiveTable(IParam param){
//		String createTableOrNot = param.getValue(
//				 ParamKey.hiveTableSwitch, "false");
//		
//		if (!createTableOrNot.equalsIgnoreCase("true"))
//			return;
//		
//		String tableFieldType = param.getValue(ParamKey.tableFieldType, "").trim().toUpperCase();
//		if (StringUtils.isEmpty(tableFieldType))
//			return;
//		
//		String[] fieldTypes = tableFieldType.split(",");
//		for (int i = 0; i < fieldTypes.length; i++) {
//			String fieldType = fieldTypes[i];
//			if (fieldType.equalsIgnoreCase("bigint")){
//				
//			}else if(fieldType.equalsIgnoreCase("string")){
//				
//			}else{
//				logger.error(String.format("unnormal fieldType:%s in the param %s",
//						tableFieldType, ParamKey.tableFieldType));
//				return;
//			}
//		}
//	}
	
	
	private void createLzoIndex(String directory) {
		logger.info("indexing lzo file on " + directory);
		try {
			Configuration cfg = DFSUtils.getConf(directory, null);
			lzoIndexer = new LzoIndexer(cfg);
			try {
				lzoIndexer.index(new Path(directory));
			} catch(IOException e) {
				logger.error(String.format(
						"HdfsWriter doPost stage index %s failed:%s,%s",
						directory, e.getMessage(), e.getCause()));
			}
		} catch (IOException e) {
			logger.error(String.format(
					"HdfsWriter doPost stage get configuration failed:%s,%s",
					e.getMessage(), e.getCause()));
		}
	}

	@Override
	public void prepare(IParam param, ISourceCounter counter) {
		dir = param.getValue(ParamKey.dir);
		prefixname = param.getValue(ParamKey.prefixname,
				"prefix");
		concurrency = param.getIntValue(ParamKey.concurrency, 1);

		if (dir.endsWith("*")) {
			dir = dir.substring(0, dir.lastIndexOf("*"));
		}
		if (dir.endsWith("/")) {
			dir = dir.substring(0, dir.lastIndexOf("/"));
		}
		
		fileType = param.getValue(ParamKey.fileType, this.fileType);
		codecClass = param.getValue(ParamKey.codecClass, "");
		if (fileType.equalsIgnoreCase("TXT_COMP")){
			suffix = DFSUtils.getCompressionSuffixMap().get(codecClass);
			if (StringUtils.isEmpty(suffix)){
				suffix = "lzo";
			}
		}
		
		if (suffix.equalsIgnoreCase("lzo")){
			lzoCompressed = true;
		}
		
		deleteFilesOnHdfs(this.dir, this.prefixname, suffix);
	}
	
	private void closeAll() {
		try {
			IOUtils.closeStream(fs);
		} catch (Exception e) {
			logger.error(String.format("HdfsWriter closing filesystem failed: %s,%s",
					e.getMessage(), e.getCause()));
		}
	}
	
	private void deleteFilesOnHdfs(String dir, String prefix, String suffix){
		try {
			fs = DFSUtils.createFileSystem(new URI(dir),
					DFSUtils.getConf(dir, null));
			
			if (concurrency == 1) {
				String file = dir + "/" + prefix;
				
				if (!StringUtils.isEmpty(suffix))
					file = file + "." + suffix; 
				
				DFSUtils.deleteFile(fs, new Path(file), true);
				
				if (lzoCompressed){
					file = file + ".index";
					if (fs.exists(new Path(file))){
						DFSUtils.deleteFile(fs, new Path(file), true);
					}
				}
			} else {
				DFSUtils.deleteFiles(fs, new Path(dir + "/" + prefix + "-*"), true, true);
			}
		} catch (Exception e) {
			logger.error(String.format("HdfsWriter Init file system failed:%s,%s", e.getMessage(),
					e.getCause()));
		} finally {
			closeAll();
		}
	}
}
