package com.dp.nebula.wormhole.plugins.writer.sftpwriter;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.dp.nebula.wormhole.common.AbstractSplitter;
import com.dp.nebula.wormhole.common.interfaces.IParam;

public class SftpDirSplitter extends AbstractSplitter {
	private static final Logger logger = Logger.getLogger(SftpDirSplitter.class);
	
	private String dir = "";
	private String prefixname = "part";
	private String fileType = "txt";
	private String suffix = "";
	private int concurrency = 5;
	
	@Override
	public void init(IParam jobParams){
		super.init(jobParams);
		
		dir = jobParams.getValue(ParamKey.dir, this.dir).trim();
		if (StringUtils.isBlank(dir)){
			logger.error("Can't find the param ["
					+ ParamKey.dir + "] in sftp-spliter-param.");
			return;
		}
		if (dir.endsWith("*")){
			dir = dir.substring(0, dir.lastIndexOf("*"));
		}
		if (dir.endsWith("/")){
			dir = dir.substring(0, dir.lastIndexOf("/"));
		}
		
		concurrency = param.getIntValue(ParamKey.concurrency, this.concurrency);
		prefixname = jobParams.getValue(ParamKey.prefixname, this.prefixname).trim();
		fileType = jobParams.getValue(ParamKey.fileType, this.fileType).trim();
		
		if (fileType.equalsIgnoreCase("gz") || fileType.equalsIgnoreCase("gzip")){
			suffix = "gz";
		}else if (fileType.equalsIgnoreCase("lzo")){
			suffix = "lzo";
		}
	}
	
	@Override
	public List<IParam> split(){
		List<IParam> v = new ArrayList<IParam>();
		String absolutePath = "";
		if (1 == concurrency){
			absolutePath = dir + "/" + prefixname;
			if (!StringUtils.isBlank(suffix)){
				absolutePath = absolutePath + "." + suffix;
			}
			logger.info(String
					.format("SftpWriter set no splitting, Use %s as absolute filename.",
							absolutePath));
			param.putValue(ParamKey.dir, absolutePath);
			v.add(param);
		}else{
			logger.info(String.format("HdfsWriter splits file to %d sub-files .",
					concurrency));
			for (int i = 0; i < concurrency; i++) {
				absolutePath = dir + "/" + prefixname + "-" + i;
				if (!StringUtils.isBlank(suffix)){
					absolutePath = absolutePath + "." + suffix;
				}
				IParam oParams = param.clone();
				oParams.putValue(ParamKey.dir, absolutePath);
				v.add(oParams);
			}
		}		
		return v;
	}
	
}
