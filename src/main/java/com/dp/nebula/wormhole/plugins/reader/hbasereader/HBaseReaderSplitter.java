package com.dp.nebula.wormhole.plugins.reader.hbasereader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import com.dp.nebula.wormhole.common.AbstractSplitter;
import com.dp.nebula.wormhole.common.interfaces.IParam;

public class HBaseReaderSplitter extends AbstractSplitter {
	private static final Logger logger = Logger.getLogger(HBaseReaderSplitter.class);

	private String tableName = null;
	private String rowkeyRange = null;
	private HBaseProxy proxy = null;

	@Override
	public void init(IParam jobParams) {
		super.init(jobParams);
		this.tableName = this.param.getValue(ParamKey.htable);
		this.rowkeyRange = this.param.getValue(ParamKey.rowkey_range, "");
	}

	@Override
	public List<IParam> split() {
		List<IParam> paramList = new ArrayList<IParam>();

		try {
			this.proxy = HBaseProxy.newProxy(tableName);
			if (StringUtils.isBlank(rowkeyRange)) {
				rowkeyRange = ",";
				param.putValue(ParamKey.rowkey_range, rowkeyRange);
			}

			Pair<byte[][], byte[][]> regionRanges = proxy.getStartEndKeys();
			if (null == regionRanges) {
				paramList = super.split();
			} else {
				String userStartKey = null;
				String userEndKey = null;

				if (!StringUtils.isBlank(rowkeyRange)) {
					if (!rowkeyRange.contains(",")) {
						throw new IllegalArgumentException(
								"rowkeyrange must be like 'startkey,endkey'");
					}

					rowkeyRange = " " + rowkeyRange + " ";
					String[] tmps = rowkeyRange.split(",");
					if (!StringUtils.isBlank(tmps[0])) {
						userStartKey = tmps[0].trim();
					}

					if (!StringUtils.isBlank(tmps[1])) {
						userEndKey = tmps[1].trim();
					}
				}

				if (null != userEndKey
						&& null != userStartKey
						&& Bytes.compareTo(userStartKey.getBytes(),
								userEndKey.getBytes()) > 0) {
					throw new IllegalArgumentException(String.format(
							"startkey %s cannot be larger than endkey %s .",
							userStartKey, userEndKey));
				}

				logger.info(String.format(
						"HBaseReader split job into %d sub-jobs .",
						regionRanges.getFirst().length));

				for (int i = 0; i < regionRanges.getFirst().length; i++) {
					IParam paramSplitted = param.clone();

					String thisStartKey = null;
					if (null != userStartKey
							&& 0 != regionRanges.getFirst()[i].length) {
						if (Bytes.compareTo(userStartKey.getBytes(),
								regionRanges.getFirst()[i]) < 0) {
							thisStartKey = Bytes.toString(regionRanges
									.getFirst()[i]);
						} else {
							thisStartKey = userStartKey;
						}
					} else if (null == userStartKey
							&& 0 == regionRanges.getFirst()[i].length) {
						thisStartKey = null;
					} else if (0 != regionRanges.getFirst()[i].length) {
						thisStartKey = Bytes
								.toString(regionRanges.getFirst()[i]);
					} else {
						thisStartKey = userStartKey;
					}

					String thisEndKey = null;
					if (null != userEndKey
							&& 0 != regionRanges.getSecond()[i].length) {
						if (Bytes.compareTo(userEndKey.getBytes(),
								regionRanges.getSecond()[i]) > 0) {
							thisEndKey = Bytes.toString(regionRanges
									.getSecond()[i]);
						} else {
							thisEndKey = userEndKey;
						}
					} else if (null == userEndKey
							&& 0 == regionRanges.getSecond()[i].length) {
						thisEndKey = null;
					} else if (null != userEndKey) {
						thisEndKey = userEndKey;
					} else {
						thisEndKey = Bytes
								.toString(regionRanges.getSecond()[i]);
					}

					paramSplitted.putValue(ParamKey.rowkey_range,
							(thisStartKey == null ? "" : thisStartKey) + ","
									+ (thisEndKey == null ? "" : thisEndKey));
					
					paramList.add(paramSplitted);
				}
			}
		} catch (IOException e) {
			paramList = super.split();
		} finally {
			try {
				if (null != proxy) {
					proxy.close();
				}
			} catch (IOException e) {
				// swallow the exception
			}
		}

		return paramList;
	}

}
