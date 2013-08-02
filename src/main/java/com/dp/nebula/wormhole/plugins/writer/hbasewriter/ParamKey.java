package com.dp.nebula.wormhole.plugins.writer.hbasewriter;

public final class ParamKey {
	/*
     * @name: htable
     * @description: hbase table name
     * @range: 
     * @mandatory: true
     * @default:
     */
	public final static String htable = "htable";
	/*
     * @name: autoFlush
     * @description: turn on/off autoFlush
     * @range: true/false
     * @mandatory: true
     * @default: false
     */
	public final static String autoFlush = "autoFlush";
	/*
     * @name: writebufferSize
     * @description: write buffer size
     * @range: [0-26214400]
     * @mandatory: true
     * @default: 1048576
     */
	public final static String writebufferSize = "writebufferSize";
	/*
     * @name: writeAheadLog
     * @description: turn on/off wal
     * @range: true/false
     * @mandatory: true
     * @default: true
     */
	public final static String writeAheadLog = "writeAheadLog";
	/*
     * @name: rowKeyIndex
     * @description: specify the rowkey index number
     * @range: 
     * @mandatory: true
     * @default:
     */
	public final static String rowKeyIndex = "rowKeyIndex";
	/*
     * @name: columnsName
     * @description: specify the column family and qualifier to write, split by comma, e.g."cf1:col1,cf2:col2"
     * @range: 
     * @mandatory: true
     * @default:
     */
	public final static String columnsName = "columnsName";
	/*
     * @name: deleteMode
     * @description: deleteMode before write data into htable, 0:do nothing, 1:delete table data, 2.truncate and create table 
     * @range: [0-2]
     * @mandatory: true
     * @default:0
     */
	public final static String deleteMode = "deleteMode";
	/*
     * @name: rollbackMode
     * @description: rollbackMode when writer failure, 0:do nothing, 1:delete table data, 2.truncate and create table 
     * @range: [0-2]
     * @mandatory: true
     * @default:0
     */
	public final static String rollbackMode = "rollbackMode";
	 /*
	  * @name: concurrency
	  * @description: concurrency of the job 
	  * @range:1-10
	  * @mandatory: false
	  * @default:1
	  */
	public final static String concurrency = "concurrency";
	
}
