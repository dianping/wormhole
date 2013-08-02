package com.dp.nebula.wormhole.plugins.reader.hbasereader;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import com.dp.nebula.wormhole.common.interfaces.ILine;

public class HBaseProxy {
	private final static Logger logger = Logger.getLogger(HBaseProxy.class);
	
	private Configuration config;
	private HTable htable;
	private HBaseAdmin admin;
	private String encode = "UTF-8";
	private byte[] startKey = null;
	private byte[] endKey = null;
	private Scan scan = null;
	private byte[][] families = null;
	private byte[][] qualifiers = null;
	private ResultScanner rs = null;
	private static final int SCAN_CACHE = 256;
	

	public static HBaseProxy newProxy(String tableName)
			throws IOException {
		return new HBaseProxy(tableName);
	}

	private HBaseProxy(String tableName) throws IOException {		
		config = HBaseConfiguration.create();
		htable = new HTable(config, tableName);
		admin = new HBaseAdmin(config);

		if (!checkStatus()) {
			throw new IllegalStateException(
					"WormHole try to build HbaseProxy failed !");
		}
	}

	public Pair<byte[][], byte[][]> getStartEndKeys() throws IOException {
		return this.htable.getStartEndKeys();
	}

	public void setEncode(String encode) {
		this.encode = encode;
		return;
	}

	public void setStartRange(byte[] startKey) {
		this.startKey = startKey;
		return;
	}

	public void setEndRange(byte[] endKey) {
		this.endKey = endKey;
		return;
	}

	public void setStartEndRange(byte[] startKey, byte[] endKey) {
		this.startKey = startKey;
		this.endKey = endKey;
		return;
	}
	
	public void prepare(String[] columns) throws IOException {
		this.scan = new Scan();
		
		if (this.startKey != null) {
			logger.info(String.format("HBaseReader set startkey to %s .",
					Bytes.toString(this.startKey)));
			scan.setStartRow(startKey);
		}
		if (this.endKey != null) {
			logger.info(String.format("HBaseReader set endkey to %s .",
					Bytes.toString(this.endKey)));
			scan.setStopRow(endKey);
		}

		this.families = new byte[columns.length][];
		this.qualifiers = new byte[columns.length][];
	
		int idx = 0;
		for (String column : columns) {
			this.families[idx] = column.split(":")[0].trim().getBytes();
			this.qualifiers[idx] = column.split(":")[1].trim().getBytes();
			scan.addColumn(this.families[idx], this.qualifiers[idx]);
			idx++;
		}

		scan.setCaching(SCAN_CACHE);
		this.rs = htable.getScanner(this.scan);
	}

	public boolean fetchLine(ILine line) throws IOException {
		if (null == this.rs) {
			throw new IllegalStateException(
					"HBase Client try to fetch data failed .");
		}

		Result result = this.rs.next();
		if (null == result) {
			return false;
		}

		for (int i = 0; i < this.families.length; i++) {
			byte[] value = result.getValue(this.families[i], this.qualifiers[i]);
			if (null == value) {
				line.addField(null);
			} else {
				line.addField(new String(value, encode));
			}
		}

		return true;
	}

	public void close() throws IOException {
		if (null != rs) {
			rs.close();
		}
		if (null != htable) {
			htable.close();
		}
		if (null != admin) {
			admin.close();
		}
	}

	private boolean checkStatus() throws IOException {
		if (!admin.isMasterRunning()) {
			throw new IllegalStateException("HBase master is not running!");
		}
		if (!admin.tableExists(htable.getTableName())) {
			throw new IllegalStateException("HBase table "
					+ Bytes.toString(htable.getTableName())
					+ " is not existed!");
		}
		if (!admin.isTableAvailable(htable.getTableName())) {
			throw new IllegalStateException("HBase table "
					+ Bytes.toString(htable.getTableName())
					+ " is not available!");
		}
		if (!admin.isTableEnabled(htable.getTableName())) {
			throw new IllegalStateException("HBase table "
					+ Bytes.toString(htable.getTableName()) + " is disable!");
		}
		return true;
	}

}

