package com.dp.nebula.wormhole.plugins.reader.hdfsreader;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.io.IOUtils;
import org.apache.log4j.Logger;

import com.dp.nebula.wormhole.common.AbstractSplitter;
import com.dp.nebula.wormhole.common.interfaces.IParam;
import com.dp.nebula.wormhole.plugins.common.DFSUtils;

public class HdfsDirSplitter extends AbstractSplitter {
	private static final Logger logger = Logger.getLogger(HdfsDirSplitter.class);
	
	private Path p = null;
	private FileSystem fs = null;
	private List<IParam> paramsList = null;
	
	@Override
	public void init(IParam jobParams){
		super.init(jobParams);
		
		String dir = jobParams.getValue(ParamKey.dir, null);
		if (dir == null) {
			logger.error("Can't find the param ["
					+ ParamKey.dir + "] in hdfs-spliter-param.");
			return;
		}
		
		if (dir.endsWith("*")) {
			dir = dir.substring(0, dir.lastIndexOf("*"));
		}
		
		p = new Path(dir);
		
		try {
			Configuration cfg = DFSUtils.getConf(dir, null);
			logger.info("fs.default.name: " + cfg.get("fs.default.name", "Not Found"));
			
			fs = DFSUtils.createFileSystem(URI.create(dir), cfg);
			
			if (!fs.exists(p)) {
				IOUtils.closeStream(fs);
				throw new Exception("the path[" + dir
						+ "] does not exist.");
			}else{
				logger.info("file " + p.toString() + " exitsts;");
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw new RuntimeException("Can't create the HDFS file system:"
					+ e.getCause());
		}
	}

	@Override
	public List<IParam> split() {
		paramsList = new ArrayList<IParam>();
		splitFilesRecursively(p);
		IOUtils.closeStream(fs);
		logger.info("the number of splitted files: " + paramsList.size());
		
		return paramsList;
	}
	
	private void splitFilesRecursively(Path path){
		try {
			FileStatus[] status = fs.listStatus(path, fileFilter);
			for (FileStatus state : status) {
				logger.debug("FileStatus	path: " + state.getPath().toString() +
						"\tlength: " + state.getLen() +
						"\tblock size: " + state.getBlockSize());
				
				if (!state.isDir()) {
					String file = state.getPath().toString();
					logger.info(ParamKey.dir + " split filename:" + file + "\tlength:" + state.getLen());
					
					IParam oParams = param.clone();
					oParams.putValue(ParamKey.dir, file);
					paramsList.add(oParams);
				} else {
					splitFilesRecursively(state.getPath());
				}
			}
		} catch (IOException e) {
			throw new RuntimeException("some errors have happened in fetching the file-status:"
					+ e.getCause());
		}
	}
	
	/* filter hidden files and LZO index files */
	private final PathFilter fileFilter = new PathFilter() {
		public boolean accept(Path p) {
			String name = p.getName();
			return !name.startsWith("_") && !name.startsWith(".") && !name.endsWith(".index");
		}
	};
}
