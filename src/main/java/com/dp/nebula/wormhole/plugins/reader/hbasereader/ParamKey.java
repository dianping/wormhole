package com.dp.nebula.wormhole.plugins.reader.hbasereader;

public final class ParamKey {
	/*
	 * @name: htable
	 * 
	 * @description:hbase table name
	 * 
	 * @range:
	 * 
	 * @mandatory: true
	 * 
	 * @default:
	 */
	public final static String htable = "htable";
	/*
	 * @name: columns_key
	 * 
	 * @description: indicate which CF:qualifier should be write, split by ","
	 * 
	 * @range:
	 * 
	 * @mandatory: true
	 * 
	 * @default:
	 */
	public final static String columns_key = "columns_key";
	/*
	 * @name: rowkey_range, split by ","
	 * 
	 * @description: range of rowkey
	 * 
	 * @range:
	 * 
	 * @mandatory: false
	 * 
	 * @default:
	 */
	public final static String rowkey_range = "rowkey_range";
	/*
	 * @name:concurrency
	 * 
	 * @description:concurrency of the job
	 * 
	 * @range:1-10
	 * 
	 * @mandatory: false
	 * 
	 * @default:1
	 */
	public final static String concurrency = "concurrency";
}
