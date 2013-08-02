package com.dp.nebula.wormhole.plugins.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.log4j.Logger;

import com.dp.nebula.wormhole.common.JobStatus;
import com.dp.nebula.wormhole.common.WormholeException;

public final class HBaseClient {
	private final static Logger LOG = Logger.getLogger(HBaseClient.class);

	private final static int BATCH_DELETE_SIZE = 1000;
	private final static int INSERT_BUFFER_SIZE = 1000;
	private final static int SCAN_CACHE_SIZE = 256;

	private static HBaseClient instance;
	private ThreadLocal<HTable> threadLocalHtable = new ThreadLocal<HTable>() {
		@Override
		protected synchronized HTable initialValue() {
			return null;
		}
	};
	private ThreadLocal<Put> threadLocalPut = new ThreadLocal<Put>();
	private ThreadLocal<List<Put>> threadLocalPutBuffer = new ThreadLocal<List<Put>>() {
		@Override
		protected synchronized List<Put> initialValue() {
			return new LinkedList<Put>();
		}
	};

	private volatile HBaseAdmin admin;
	private String tableName;
	private Configuration conf;
	private boolean autoFlush;
	private int writeBufferSize;
	private boolean writeAheadLog;

	private HBaseClient() {
	}

	public void initialize(String htable, boolean autoFlush,
			int writeBufferSize, boolean writeAheadLog) {
		this.tableName = htable;
		conf = HBaseConfiguration.create();
		try {
			admin = new HBaseAdmin(conf);
			checkStatus(tableName);
		} catch (IOException e) {
			throw new WormholeException(e,
					JobStatus.PRE_CHECK_FAILED.getStatus());
		}
		this.autoFlush = autoFlush;
		this.writeBufferSize = writeBufferSize;
		this.writeAheadLog = writeAheadLog;
		LOG.info("HBaseClient was initialized. tableName is " + tableName);
	}

	public static synchronized HBaseClient getInstance() {
		if (instance == null) {
			instance = new HBaseClient();
		}
		return instance;
	}

	@SuppressWarnings("deprecation")
	public void setRowKey(byte[] rowkey) {
		Put put = new Put(rowkey);
		put.setWriteToWAL(writeAheadLog);
		setPut(put);
	}

	public void addColumn(byte[] family, byte[] qualifier, byte[] value) {
		getPut().add(family, qualifier, value);
	}

	public void insert() throws IOException {
		getPutBuffer().add(getPut());
		if (getPutBuffer().size() >= INSERT_BUFFER_SIZE) {
			getHTable().put(getPutBuffer());
			clearPutBuffer();
		}
	}

	public void close() throws IOException {
		HTable table = threadLocalHtable.get();
		if (table != null) {
			table.close();
			table = null;
			threadLocalHtable.remove();
			closeHBaseAdmin();
		}
	}
	
	public synchronized void closeHBaseAdmin() throws IOException{
		if (admin != null) {
			admin.close();
			admin = null;
		}
		
	}

	public void flush() throws IOException {
		if (getPutBuffer().size() > 0) {
			getHTable().put(getPutBuffer());
			clearPutBuffer();
		}
	}

	private void checkStatus(String htable) throws IOException {
		if (!admin.isMasterRunning()) {
			throw new WormholeException("hbase master is not running",
					JobStatus.PRE_CHECK_FAILED.getStatus());
		}
		if (!admin.isTableAvailable(htable)) {
			throw new WormholeException(String.format(
					"htable %s is not available", htable),
					JobStatus.PRE_CHECK_FAILED.getStatus());
		}
		if (!admin.tableExists(htable)) {
			throw new WormholeException(String.format(
					"htable %s doesn't exist", htable),
					JobStatus.PRE_CHECK_FAILED.getStatus());
		}
		if (!admin.isTableEnabled(htable)) {
			throw new WormholeException(String.format("htable %s is disabled",
					htable), JobStatus.PRE_CHECK_FAILED.getStatus());
		}
	}

	public void deleteTableData(String table) throws IOException {
		HTableInterface htable = getHTable();
		Scan s = new Scan();
		s.setCaching(SCAN_CACHE_SIZE);
		ResultScanner rs = htable.getScanner(s);
		List<Delete> deleteList = new ArrayList<Delete>(BATCH_DELETE_SIZE);

		LOG.info(String.format("start to delete table %s's data", table));
		for (Result r : rs) {
			deleteList.add(new Delete(r.getRow()));
			if (deleteList.size() >= BATCH_DELETE_SIZE) {
				htable.delete(deleteList);
				deleteList.clear();
			}
		}
		if (deleteList.size() > 0) {
			htable.delete(deleteList);
		}
		rs.close();
	}

	public void truncateTable(String table) throws IOException {
		HTableDescriptor tableDesc = getHTable().getTableDescriptor();
		admin.disableTable(table);
		admin.deleteTable(table);
		admin.createTable(tableDesc);
	}

	private HTable getHTable() throws IOException {
		HTable htable = threadLocalHtable.get();
		if (htable == null) {
			htable = new HTable(conf, tableName);
			htable.setAutoFlush(autoFlush);
			htable.setWriteBufferSize(writeBufferSize);
			threadLocalHtable.set(htable);
		}
		return htable;
	}

	private Put getPut() {
		return threadLocalPut.get();
	}

	private void setPut(Put put) {
		threadLocalPut.set(put);
	}

	private List<Put> getPutBuffer() {
		return threadLocalPutBuffer.get();
	}

	private void clearPutBuffer() {
		getPutBuffer().clear();
	}
}
