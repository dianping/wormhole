package com.dp.nebula.wormhole.plugins.writer.hbasewriter;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.dp.nebula.wormhole.common.AbstractPlugin;
import com.dp.nebula.wormhole.common.JobStatus;
import com.dp.nebula.wormhole.common.WormholeException;
import com.dp.nebula.wormhole.common.interfaces.ILine;
import com.dp.nebula.wormhole.common.interfaces.ILineReceiver;
import com.dp.nebula.wormhole.common.interfaces.IWriter;
import com.dp.nebula.wormhole.plugins.common.HBaseClient;
import com.google.common.base.Preconditions;

public class HBaseWriter extends AbstractPlugin implements IWriter {
	private final static Logger LOG = Logger.getLogger(HBaseWriter.class);

	private final static String DEFAULT_ENCODING = "UTF-8";
	private final static int MININUM_FIELD_NUM = 2;
	
	private int rowKeyIndex;
	private String columnsName;
	private byte[][] columnFamilies;
	private byte[][] qualifiers;
	private HBaseClient client;

	@Override
	public void init() {
		rowKeyIndex = getParam().getIntValue(ParamKey.rowKeyIndex, 0);
		columnsName = getParam().getValue(ParamKey.columnsName);
		parseColumnsMapping(columnsName);
	}

	private void parseColumnsMapping(String columnsName) {
		if (StringUtils.isBlank(columnsName)) {
			throw new IllegalArgumentException("columns names can not be empty");
		}
		String[] columnsNameArray = StringUtils.split(columnsName, ',');
		columnFamilies = new byte[columnsNameArray.length][];
		qualifiers = new byte[columnsNameArray.length][];
		
		for (int i = 0; i < columnsNameArray.length; i++) {
			String[] parts = StringUtils.split(columnsNameArray[i], ':');
			if (!(parts.length == 2)) {
				throw new IllegalArgumentException(String.format(
						"column name %s must be specified as cf:qualifier",
						columnsNameArray[i]));
			}
			try {
				columnFamilies[i] = parts[0].getBytes(
						DEFAULT_ENCODING);
				qualifiers[i] = parts[1].getBytes(
						DEFAULT_ENCODING);
			} catch (UnsupportedEncodingException e) {
				throw new WormholeException(e,
						JobStatus.READ_FAILED.getStatus());
			}
		}
	}

	@Override
	public void connection() {
		client = HBaseClient.getInstance();
		Preconditions.checkNotNull(client);
	}

	@Override
	public void write(ILineReceiver lineReceiver) {
		ILine line;
		try {
			while ((line = lineReceiver.receive()) != null) {
				int fieldNum = line.getFieldNum();
				if (fieldNum < MININUM_FIELD_NUM) {
					LOG.warn("field number is less than " + MININUM_FIELD_NUM + " consider it as an empty line:" + line.toString(','));
					continue;
				}
				String rowKey = line.getField(rowKeyIndex);
				if (StringUtils.isEmpty(rowKey)) {
					throw new WormholeException(
							"hbase rowkey should not be empty",
							JobStatus.WRITE_DATA_EXCEPTION.getStatus());
				}
				client.setRowKey(rowKey.getBytes(DEFAULT_ENCODING));
				for (int i = 0; i < rowKeyIndex; i++) {
					if (line.getField(i) == null) {
						continue;
					}
					client.addColumn(columnFamilies[i], qualifiers[i], line
							.getField(i).getBytes(DEFAULT_ENCODING));
				}
				for (int i = rowKeyIndex + 1; i < fieldNum; i++) {
					if (line.getField(i) == null) {
						continue;
					}
					client.addColumn(columnFamilies[i-1], qualifiers[i-1], line
							.getField(i).getBytes(DEFAULT_ENCODING));
				}
				client.insert();
				getMonitor().increaseSuccessLines();
			}
		} catch (Exception e) {
			throw new WormholeException(e, JobStatus.WRITE_DATA_EXCEPTION.getStatus());
		}
	}

	@Override
	public void commit() {
		try {
			client.flush();
		} catch (IOException e) {
			throw new WormholeException(e, JobStatus.WRITE_DATA_EXCEPTION.getStatus());
		}
	}

	@Override
	public void finish() {
		try {
			client.close();
		} catch (IOException e) {
			throw new WormholeException(e, JobStatus.WRITE_DATA_EXCEPTION.getStatus());
		}
	}
}
