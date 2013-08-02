package com.dp.nebula.wormhole.plugins.reader.hivereader;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.dp.nebula.wormhole.common.AbstractSplitter;
import com.dp.nebula.wormhole.common.JobStatus;
import com.dp.nebula.wormhole.common.WormholeException;
import com.dp.nebula.wormhole.common.interfaces.IParam;
import com.dp.nebula.wormhole.plugins.common.DFSUtils;

public class HiveReaderSplitter extends AbstractSplitter {
	private static final Logger LOG = Logger
			.getLogger(HiveReaderSplitter.class);

	private String mode = HiveReaderMode.READ_FROM_HIVESERVER.getMode();
	private String sourceDir;

	@Override
	public void init(IParam jobParams) {
		super.init(jobParams);
		mode = param.getValue(ParamKey.mode, mode);
		sourceDir = param.getValue(ParamKey.dataDir);
	}

	@Override
	public List<IParam> split() {
		List<IParam> result = new ArrayList<IParam>();
		HiveReaderMode readerMode = HiveReaderMode.valueOf(mode);

		switch (readerMode) {
		case READ_FROM_HIVESERVER:
			result.add(param);
			break;
		case READ_FROM_HDFS:
			FileSystem fs = null;
			try {
				Configuration conf = DFSUtils.getConf(sourceDir, null);
				fs = DFSUtils.createFileSystem(new URI(sourceDir), conf);
				FileStatus[] files = fs.listStatus(new Path(sourceDir));
				for (FileStatus fileStatus : files) {
					IParam p = param.clone();
					p.putValue(ParamKey.dataDir, fileStatus.getPath()
							.toString());
					result.add(p);
				}
			} catch (Exception e) {
				throw new WormholeException(e,
						JobStatus.READ_FAILED.getStatus());
			} finally {
				if (fs != null) {
					try {
						fs.close();
					} catch (IOException e) {
					}
				}
			}
			break;

		}
		LOG.info("splitted files num:" + result.size());

		return result;
	}

}
