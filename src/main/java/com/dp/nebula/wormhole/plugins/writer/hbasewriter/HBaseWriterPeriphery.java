package com.dp.nebula.wormhole.plugins.writer.hbasewriter;

import java.io.IOException;

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

	private final static int DEFAULT_WRITE_BUFFER_SIZE = 1024 * 1024;

	private String htable;
	private int deleteMode;
	private int rollbackMode;
	private int concurrency;
	private Boolean autoFlush;
	private Boolean writeAheadLog;
	private int writebufferSize;
	private HBaseClient client;

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
		Preconditions.checkArgument(writebufferSize > 0
				&& writebufferSize <= 32 * 1024 * 1024,
				"write buffer size must be within 0-32MB");

		client = HBaseClient.getInstance();
		client.initialize(htable, autoFlush, writebufferSize,
				writeAheadLog);
		deleteTableByMode(deleteMode);
	}

	@Override
	public void doPost(IParam param, ITargetCounter counter) {
		LOG.info("start to close HBaseClient");
		if (client != null) {
			try {
				client.close();
			} catch (IOException e) {
			}
		}
	}

	@Override
	public void rollback(IParam param) {
		LOG.info("start to execute `delete table` by rollbackMode on rollback stage");
		deleteTableByMode(rollbackMode);
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
