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
	/*
	  * @name: write_sleep
	  * @description: slow the speed of write 
	  * @range:
	  * @mandatory: false
	  * @default: false
	  */
	public final static String write_sleep = "write_sleep";
	/*
	  * @name: wait_time
	  * @description: wait_time of the write 
	  * @range:
	  * @mandatory: 
	  * @default:1000
	  */
	public final static String wait_time = "wait_time"; 
	/*
	  * @name: num_to_wait
	  * @description: num_to_wait of the write 
	  * @range:
	  * @mandatory: 
	  * @default:1000
	  */
	public final static String num_to_wait = "num_to_wait"; 
	
	/*
	  * @name: putTimeStamp
	  * @description: seconds to decrease the current_timestamp
	  * @range:
	  * @mandatory:
	  * @default: 5
	  */
	public final static String secondsDecTimeStamp = "secondsDecTimeStamp"; 
	
	/*
	  * @name: isDeleteData
	  * @description: after insert data, do you want to delete old data
	  * @range:
	  * @mandatory: false
	  * @default: true
	  */
	public final static String isDeleteData = "isDeleteData"; 
	/*
	  * @name: isMajor_compact
	  * @description: after insert&delete data, do you want to compact
	  * @range:
	  * @mandatory: true
	  * @default: true
	  */
	public final static String isMajor_Compact = "isMajor_Compact";
	/*
	  * @name: doPostDeleteDataFromHive
	  * @description:whether to delete data at doPost stage from hive data
	  * @range:true,false
	  * @mandatory: true
	  * @default:false
	  */
	public final static String doPostDeleteDataFromHive = "doPostDeleteDataFromHive";
	/*
	  * @name: hiveConnectionUrl
	  * @description:hive connection url
	  * @range:
	  * @mandatory: false
	  * @default:jdbc:hive://10.1.1.161:10000/default
	  */
	public final static String hiveConnectionUrl = "hiveConnectionUrl";
	/*
	  * @name: selectRowkeysQuery
	  * @description: hive select query to fetch rowkeys
	  * @range:
	  * @mandatory: true
	  * @default: 
	  */
	public final static String selectRowkeysQuery = "selectRowkeysQuery"; 
}
