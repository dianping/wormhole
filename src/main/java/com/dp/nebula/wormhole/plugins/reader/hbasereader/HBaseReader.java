package com.dp.nebula.wormhole.plugins.reader.hbasereader;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.dp.nebula.wormhole.common.AbstractPlugin;
import com.dp.nebula.wormhole.common.JobStatus;
import com.dp.nebula.wormhole.common.WormholeException;
import com.dp.nebula.wormhole.common.interfaces.ILine;
import com.dp.nebula.wormhole.common.interfaces.ILineSender;
import com.dp.nebula.wormhole.common.interfaces.IReader;

public class HBaseReader extends AbstractPlugin implements IReader {
	private static final Logger logger = Logger.getLogger(HBaseReader.class);

	private String tableName = null;
	private String columns = null;
	private String rowkeyRange = null;
	private HBaseProxy proxy = null;

	@Override
	public void init() {
		this.tableName = getParam().getValue(ParamKey.htable, "");
		this.columns = getParam().getValue(ParamKey.columns_key, "");
		this.rowkeyRange = getParam().getValue(ParamKey.rowkey_range, "");

		try {
			proxy = HBaseProxy.newProxy(tableName);
		} catch (IOException e) {
			try {
				if (null != proxy) {
					proxy.close();
				}
			} catch (IOException e1) {
			}
			logger.error(e);
			throw new WormholeException(e.getMessage(),
					JobStatus.READ_FAILED.getStatus());
		}
	}

	@Override
	public void connection() {
		logger.info("HBaseReader start to connect to HBase .");
		if (StringUtils.isBlank(rowkeyRange)) {
			logger.info("HBaseReader prepare to query all records . ");
			proxy.setStartEndRange(null, null);
		} else {
			rowkeyRange = " " + rowkeyRange + " ";
			String[] pair = rowkeyRange.split(",");
			if (null == pair || 0 == pair.length) {
				logger.info("HBaseReader prepare to query all records . ");
				proxy.setStartEndRange(null, null);
			} else {
				String start = StringUtils.isBlank(pair[0].trim()) ? null
						: pair[0].trim();
				String end = StringUtils.isBlank(pair[1].trim()) ? null
						: pair[1].trim();
				logger.info(String.format(
						"HBaseReader prepare to query records [%s, %s) .",
						(start == null ? "-infinite" : start),
						(end == null ? "+infinite" : end)));
				proxy.setStartEndRange(
						(start == null ? null : start.getBytes()),
						(end == null ? null : end.getBytes()));
			}
		}
	}

	@Override
	public void read(ILineSender lineSender) {
		try {
			proxy.prepare(columns.split(","));
			ILine line = lineSender.createNewLine();
			while (proxy.fetchLine(line)) {
				Boolean flag = lineSender.send(line);
				if(getMonitor() != null) {
					if (flag){
						getMonitor().increaseSuccessLines();
					}else{
						getMonitor().increaseFailedLines();
					}
				}
				line = lineSender.createNewLine();
			}
			lineSender.flush();
		} catch (IOException e) {
			logger.error("HBase Reader fetch line error " + e.toString());
			throw new WormholeException(e, JobStatus.READ_DATA_EXCEPTION.getStatus());
		} finally {
			try {
				proxy.close();
			} catch (IOException e) {
			}
		}
	}
}
