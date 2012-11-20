package com.dp.nebula.wormhole.plugins.writer.hdfswriter;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.dp.nebula.wormhole.common.AbstractSplitter;
import com.dp.nebula.wormhole.common.interfaces.IParam;
import com.dp.nebula.wormhole.plugins.common.DFSUtils;

public class HdfsDirSplitter extends AbstractSplitter {
	private Logger logger = Logger.getLogger(HdfsDirSplitter.class);
	
	private Path p = null;
	private String prefix = "prefix";
	private int concurrency = 5;
	private String codecClass = "";
	private String fileType = "TXT";
	
	@Override
	public void init(IParam jobParams) {
		super.init(jobParams);
		
		String dir = param.getValue(ParamKey.dir);

		if (dir.endsWith("*")) {
			dir = dir.substring(0, dir.lastIndexOf('*'));
		}
		if (dir.endsWith("/")) {
			dir = dir.substring(0, dir.lastIndexOf('/'));
		}
		
		codecClass = param.getValue(ParamKey.codecClass, this.codecClass);

		p = new Path(dir);
		
		fileType = param.getValue(ParamKey.fileType, this.fileType);

        prefix = param.getValue(ParamKey.prefixname, prefix);

		concurrency = param.getIntValue(ParamKey.concurrency,
				this.concurrency);
	}
	
	@Override
	public List<IParam> split() {
		String suffix = "";
		if (fileType.equalsIgnoreCase("TXT_COMP")){
			suffix = DFSUtils.getCompressionSuffixMap().get(codecClass);
			if (StringUtils.isEmpty(suffix)){
				suffix = "lzo"; 
			}
		}
		
		List<IParam> v = new ArrayList<IParam>();
		if (1 == concurrency){
			if (!StringUtils.isEmpty(suffix)){
				prefix = prefix + "." + suffix;
			}
			
			logger.info(String
					.format("HdfsWriter set no splitting, Use %s as absolute filename .",
							p.toString() + "/" + prefix));
			param.putValue(ParamKey.dir, p.toString() + "/" + prefix);
			v.add(param);
		}else{
			logger.info(String.format("HdfsWriter splits file to %d sub-files .",
					concurrency));
			for (int i = 0; i < concurrency; i++) {
				String file = p.toString() + "/" + prefix + "-" + i;
				if (!StringUtils.isEmpty(suffix)){
					file = file + "." + suffix;
				}
				
				IParam oParams = param.clone();
				oParams.putValue(ParamKey.dir, file);
				v.add(oParams);
			}
		}
		return v;
	}
}
