package com.dp.nebula.wormhole.plugins.writer.hbasewriter;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.apache.log4j.Logger;

import com.dp.nebula.wormhole.common.JobStatus;
import com.dp.nebula.wormhole.common.WormholeException;
import com.dp.nebula.wormhole.common.interfaces.IParam;
import com.dp.nebula.wormhole.common.interfaces.ISourceCounter;
import com.dp.nebula.wormhole.common.interfaces.ITargetCounter;
import com.dp.nebula.wormhole.common.interfaces.IWriterPeriphery;
import com.dp.nebula.wormhole.plugins.common.HBaseClient;
import com.google.common.base.Preconditions;

public class HBaseWriterPeriphery implements IWriterPeriphery {
	private final static Logger LOG = Logger
			.getLogger(HBaseWriterPeriphery.class);

	private final static String HIVE_DRIVER_NAME = "org.apache.hadoop.hive.jdbc.HiveDriver";
	private final static int DEFAULT_WRITE_BUFFER_SIZE = 1024 * 1024;
	private final static int DELETE_BATCH_SIZE = 1000;

	private String htable;
	private int deleteMode;
	private int rollbackMode;
	private int concurrency;
	private Boolean autoFlush;
	private Boolean writeAheadLog;
	private int writebufferSize;
	private HBaseClient client;
	private Boolean isDeleteData;
	private long timeStampDel;
	private Boolean isMajor_Compact;
	private Boolean doPostDeleteDataFromHive;
	private String hiveConnectionUrl;
	private String selectRowkeysQuery;

	@Override
	public void prepare(IParam param, ISourceCounter counter) {
		htable = param.getValue(ParamKey.htable);
		concurrency = param.getIntValue(ParamKey.concurrency, 1);
		deleteMode = param.getIntValue(ParamKey.deleteMode, 0);
		Preconditions.checkArgument(deleteMode >= 0 && deleteMode <= 2,
				"deleteMode must be between 0 and 2");
		rollbackMode = param.getIntValue(ParamKey.rollbackMode, 0);
		Preconditions.checkArgument(rollbackMode >= 0 && rollbackMode <= 2,
				"rollbackMode must be between 0 and 2");

		autoFlush = param.getBooleanValue(ParamKey.autoFlush, false);
		writeAheadLog = param.getBooleanValue(ParamKey.writeAheadLog, true);
		writebufferSize = param.getIntValue(ParamKey.writebufferSize,
				DEFAULT_WRITE_BUFFER_SIZE);
		hiveConnectionUrl = param.getValue(ParamKey.hiveConnectionUrl);
		selectRowkeysQuery = param.getValue(ParamKey.selectRowkeysQuery);

		Preconditions.checkArgument(writebufferSize > 0
				&& writebufferSize <= 32 * 1024 * 1024,
				"write buffer size must be within 0-32MB");

		client = HBaseClient.getInstance();
		client.initialize(htable, autoFlush, writebufferSize, writeAheadLog);
		deleteTableByMode(deleteMode);
	}

	@Override
	public void doPost(IParam param, ITargetCounter counter, int faildSize) {
		doPostDeleteDataFromHive = param.getBooleanValue(
				ParamKey.doPostDeleteDataFromHive, false);
		if (doPostDeleteDataFromHive) {
			try {
				LOG.info("start to delete data from hive execution query:" + selectRowkeysQuery);
				deleteRowkeysFromHive();
			} catch (Exception e) {
				throw new WormholeException(e,
						JobStatus.POST_WRITE_FAILED.getStatus());
			}
		}

		isDeleteData = param.getBooleanValue(ParamKey.isDeleteData);
		timeStampDel = param.getLongValue(ParamKey.secondsDecTimeStamp);
		isMajor_Compact = param.getBooleanValue(ParamKey.isMajor_Compact);

		// add by mt
		if (faildSize == 0) {
			try {
				if (isDeleteData) {
					LOG.info("start to delete data from table...");
					client.deleteTableDataByTimestamp(htable, timeStampDel);
				}
			} catch (IOException e) {
				LOG.error("delete data from table Error: " + e.getMessage());
				throw new WormholeException(e,
						JobStatus.POST_WRITE_FAILED.getStatus());
			} catch (InterruptedException e) {
				LOG.error("delete data from table Error: " + e.getMessage());
				throw new WormholeException(e,
						JobStatus.POST_WRITE_FAILED.getStatus());
			}
		} else {
			LOG.info("HBase write error, skip doPost(delete table data)...");
		}

		if (isMajor_Compact) {
			try {
				client.major_Compact(htable);
			} catch (Exception e) {
				LOG.info(String.format("table  %s major_compact exception",
						htable));
				throw new WormholeException(e,
						JobStatus.POST_WRITE_FAILED.getStatus());
			}
		}

		LOG.info("start to close HBaseClient");
		if (client != null) {
			try {
				client.close();
			} catch (IOException e) {
				LOG.error("close client Error: " + e.getMessage());
				throw new WormholeException(e,
						JobStatus.POST_WRITE_FAILED.getStatus());
			}
		}
	}

	@Override
	public void rollback(IParam param) {
		LOG.info("start to execute `delete table` by rollbackMode on rollback stage");
		deleteTableByMode(rollbackMode);
	}

	private void deleteRowkeysFromHive() throws Exception {
		List<String> rowkeysToDelete = new ArrayList<String>();
		Class.forName(HIVE_DRIVER_NAME);
		
		Connection conn = DriverManager
				.getConnection(hiveConnectionUrl, "", "");
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery(selectRowkeysQuery);
		int columnCount = rs.getMetaData().getColumnCount();
		if (columnCount != 1) {
			throw new IllegalArgumentException(
					"it must be only one column(rowkey) specified. columnCount : "
							+ columnCount);
		}
		
		int rowCount = 0;
		while (rs.next()) {
			rowCount++;
			rowkeysToDelete.add(rs.getString(1));
			if (rowkeysToDelete.size() >= DELETE_BATCH_SIZE) {
				client.deleteTableDataByRowkeys(rowkeysToDelete);
				LOG.info("deleted rowkey count " + rowCount);
				rowkeysToDelete.clear();
			}
		}
		
		if (rowkeysToDelete.size() > 0) {
			client.deleteTableDataByRowkeys(rowkeysToDelete);
			LOG.info("deleted rowkey count " + rowCount);
		}
		rs.close();
		stmt.close();
		conn.close();
	}

	private void deleteTableByMode(int mode) {
		if (0 == mode) {
			LOG.info("mode 0, do nothing with table data");
		} else if (1 == mode) {
			try {
				LOG.info("mode 1, delete table data");
				client.deleteTableData(htable);
			} catch (IOException e) {
				throw new WormholeException(e,
						JobStatus.PRE_CHECK_FAILED.getStatus());
			}
		} else if (2 == mode) {
			try {
				LOG.info("mode 2, truncate and recreate table");
				client.truncateTable(htable);
			} catch (IOException e) {
				throw new WormholeException(e,
						JobStatus.PRE_CHECK_FAILED.getStatus());
			}
		}
	}
}
