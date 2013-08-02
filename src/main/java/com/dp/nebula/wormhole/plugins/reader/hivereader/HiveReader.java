package com.dp.nebula.wormhole.plugins.reader.hivereader;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.sql.SQLException;

import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.log4j.Logger;

import com.dp.nebula.wormhole.common.AbstractPlugin;
import com.dp.nebula.wormhole.common.JobStatus;
import com.dp.nebula.wormhole.common.WormholeException;
import com.dp.nebula.wormhole.common.interfaces.ILine;
import com.dp.nebula.wormhole.common.interfaces.ILineSender;
import com.dp.nebula.wormhole.common.interfaces.IReader;
import com.dp.nebula.wormhole.plugins.common.DFSUtils;

public class HiveReader extends AbstractPlugin implements IReader {
	private static final Logger LOG = Logger.getLogger(HiveReader.class);
	private static final char FIELD_SEPARATOR = '\001';

	private String path = "jdbc:hive://10.1.1.161:10000/default";
	private String username = "";
	private String password = "";
	private String sql = "";
	private String filePath;
	private String mode = HiveReaderMode.READ_FROM_HIVESERVER.getMode();

	private HiveJdbcClient client;
	private FileSystem fs;
	private Configuration conf;

	@Override
	public void init() {
		mode = getParam().getValue(ParamKey.mode, mode);
		path = getParam().getValue(ParamKey.path, path);
		username = getParam().getValue(ParamKey.username, username);
		password = getParam().getValue(ParamKey.password, password);
		sql = getParam().getValue(ParamKey.sql, sql).trim();
		filePath = getParam().getValue(ParamKey.dataDir);
	}

	@Override
	public void connection() {
		if (mode.equals(HiveReaderMode.READ_FROM_HIVESERVER.getMode())) {
			client = new HiveJdbcClient.Builder(path).username(username)
					.password(password).sql(sql).build();
			client.initialize();
		}
	}

	@Override
	public void read(ILineSender lineSender) {
		if (mode.equals(HiveReaderMode.READ_FROM_HIVESERVER.getMode())) {
			readFromHiveServer(lineSender);
		} else if (mode.equals(HiveReaderMode.READ_FROM_HDFS.getMode())) {
			LOG.info("start to read " + filePath);
			readFromHdfs(lineSender);
		}
	}

	private void readFromHdfs(ILineSender lineSender) {
		FSDataInputStream in = null;
		CompressionCodecFactory factory;
		CompressionCodec codec;
		CompressionInputStream cin = null;
		LineIterator itr = null;
		try {
			conf = DFSUtils.getConf(filePath, null);
			fs = DFSUtils.createFileSystem(new URI(filePath), conf);
			in = fs.open(new Path(filePath));
			factory = new CompressionCodecFactory(conf);
			codec = factory.getCodec(new Path(filePath));
			if (codec == null) {
				LOG.info("codec not found, using text file reader");
				itr = new LineIterator(new BufferedReader(
						new InputStreamReader(in)));
			} else {
				LOG.info("found code " + codec.getClass());
				cin = codec.createInputStream(in);
				itr = new LineIterator(new BufferedReader(
						new InputStreamReader(cin)));
			}
			while (itr.hasNext()) {
				ILine oneLine = lineSender.createNewLine();
				String line = itr.nextLine();
				String[] parts = StringUtils.split(line, FIELD_SEPARATOR);
				for (int i = 0; i < parts.length; i++) {
					oneLine.addField(parts[i], i);
				}
				boolean flag = lineSender.send(oneLine);
				if (flag) {
					getMonitor().increaseSuccessLines();
				} else {
					getMonitor().increaseFailedLines();
				}
			}
			lineSender.flush();

		} catch (Exception e) {
			LOG.error(e.getCause());
			throw new WormholeException(e,
					JobStatus.READ_DATA_EXCEPTION.getStatus());
		} finally {
			if (itr != null) {
				itr.close();
			}
			try {
				if (cin != null) {
					cin.close();
				}
				if (in != null) {
					in.close();
				}
				if (fs != null) {
					fs.close();
				}
			} catch (IOException e) {
				LOG.warn(e);
				// swallow the exception
			}
		}
	}

	private void readFromHiveServer(ILineSender lineSender) {
		try {
			client.processSelectQuery(lineSender, getMonitor());
		} catch (SQLException e) {
			throw new WormholeException(e,
					JobStatus.READ_DATA_EXCEPTION.getStatus());
		}
	}

	@Override
	public void finish() {
		if (client != null) {
			client.close();
		}
	}
}
