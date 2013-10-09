package com.dp.nebula.wormhole.plugins.common;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.coprocessor.example.BulkDeleteProtocol;
import org.apache.hadoop.hbase.coprocessor.example.BulkDeleteResponse;
import org.apache.hadoop.hbase.coprocessor.example.BulkDeleteProtocol.DeleteType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;

import com.dp.nebula.wormhole.common.JobStatus;
import com.dp.nebula.wormhole.common.WormholeException;

public final class HBaseClient {
	private final static Logger LOG = Logger.getLogger(HBaseClient.class);

	private final static int BATCH_DELETE_SIZE = 1000;
	private final static int INSERT_BUFFER_SIZE = 1000;
	private final static int SCAN_CACHE_SIZE = 1000;

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

	@SuppressWarnings("deprecation")
	public void setRowKeyWithTs(byte[] rowkey, long ts) {
		Put put = new Put(rowkey, ts);
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

	public synchronized void closeHBaseAdmin() throws IOException {
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

	/*
	 * delete table data by timeStamp added by mt
	 */
	public void deleteTableDataByTimestamp(String table, long timeStampDel)
			throws IOException, InterruptedException {
		HTableInterface htbale = getHTable();
		int count = 0;
		long current_timestamp = System.currentTimeMillis();
		long end_timestamp = current_timestamp - timeStampDel * 1000;
		long period = 10000;
		long delStartTime = System.currentTimeMillis();
		Scan s = new Scan();
		s.setCaching(SCAN_CACHE_SIZE);
		s.setMaxVersions();
		s.setTimeRange(0, end_timestamp); // 扫描出 需要删除的row，条件：指定的timestamp范围
		ResultScanner rs = htbale.getScanner(s);
		List<Delete> deleteList = new ArrayList<Delete>(BATCH_DELETE_SIZE);

		LOG.info(String.format(
				"start to delete table %s data by timestamp range [ 0 ~ %d ]",
				table, end_timestamp));
		for (Result r : rs) {
			Delete dataDelete = new Delete(r.getRow());
			KeyValue[] kv = r.raw();
			for (int i = 0; i < kv.length; i++) {
				dataDelete = dataDelete.deleteColumns(kv[i].getFamily(),
						kv[i].getQualifier(), kv[i].getTimestamp()); // use
																		// deleteColumns(f,q)
																		// means:
				// delete all versions, but
				// deleteColumn() delete the
				// latest version
			}
			deleteList.add(dataDelete);
			count++;
			if ((count % period) == 0) {
				LOG.info(String.format(
						"has deleted table %s data for %d rows!", table, count));
			}
			if (deleteList.size() >= BATCH_DELETE_SIZE) {
				htbale.delete(deleteList);
				deleteList.clear();
			}
		}
		if (deleteList.size() > 0) {
			htbale.delete(deleteList);
		}
		rs.close();
		long delEndTime = System.currentTimeMillis();
		LOG.info(String
				.format("deleted table %s data by timestamp %d for %d rows *** used %d ms ...",
						table, timeStampDel, count, (delEndTime - delStartTime)));
	}

	private long invokeBulkDeleteProtocol(HTableInterface ht, final Scan scan,
			final int rowBatchSize, final byte deleteType, final Long timeStamp){
		long noOfDeletedRows = 0L;
		long period = 10000;
		Batch.Call<BulkDeleteProtocol, BulkDeleteResponse> callable = new Batch.Call<BulkDeleteProtocol, BulkDeleteResponse>() {
			public BulkDeleteResponse call(BulkDeleteProtocol instance)
					throws IOException {
				return instance.delete(scan, deleteType, timeStamp,
						rowBatchSize);
			}
		};
		try {
			Map<byte[], BulkDeleteResponse> result = ht.coprocessorExec(
					BulkDeleteProtocol.class, scan.getStartRow(),
					scan.getStopRow(), callable);
			for (BulkDeleteResponse response : result.values()) {
				noOfDeletedRows += response.getRowsDeleted();
				if ((noOfDeletedRows % period) == 0) {
					LOG.info(String.format("has deleted table data for %d rows!",
							noOfDeletedRows));
				}
			}
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		} catch (Throwable e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return noOfDeletedRows;
	}

	public void deleteTableDataByTimestamp_coprocessor(String table,
			long timeStampDel) throws IOException, InterruptedException,Throwable {
		HTableInterface htbale = getHTable();
		long current_timestamp = System.currentTimeMillis();
		long end_timestamp = current_timestamp - timeStampDel * 1000;
		long delStartTime = System.currentTimeMillis();
		Scan scan = new Scan();
		scan.setTimeRange(0, end_timestamp);
		scan.setMaxVersions();
		LOG.info(String.format(
				"start to delete table %s data by timestamp range [ 0 ~ %d ]",
				table, end_timestamp));
		long rowsDelete = invokeBulkDeleteProtocol(htbale, scan, 1000,
				DeleteType.COLUMN, null);
		long delEndTime = System.currentTimeMillis();
		LOG.info(String
				.format("deleted table %s data by timestamp %d for %d rows *** used %d ms ...",
						table, timeStampDel, rowsDelete, (delEndTime - delStartTime)));
	}

	public void major_Compact(String table) throws IOException,
			InterruptedException {
		LOG.info(String.format("start to compact table %s ...", table));
		HBaseAdmin hAdmin = getAdmin();
		if (null != hAdmin) {
			hAdmin.majorCompact(table);
			LOG.info(String.format("compact table %s completed...", table));
			hAdmin.close();
		} else {
			LOG.error(String.format(
					" compact: can not get HbaseAdmin for table %s", table));
		}
	}

	// end of add by mt

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

	private HBaseAdmin getAdmin() throws IOException {
		if (admin == null) {
			admin = new HBaseAdmin(conf);
		}
		return admin;
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