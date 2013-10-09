package com.dp.nebula.wormhole.plugins.reader.hdfsreader;

public final class ParamKey {
	/*
	 * @name: dir
	 * @description: hdfs path, format like: hdfs://ip:port/path , file:////data/tmp/
	 * @range:
	 * @mandatory: true 
	 * @default:
	 */
	public final static String dir = "dir";
	/*
	 * @name: fieldSplit
	 * @description: field separator
	 * @range:
	 * @mandatory: false 
	 * @default:\t
	 */
	public final static String fieldSplit = "field_split";
	/*
	 * @name: encoding 
	 * @description: hdfs encode
	 * @range:UTF-8|GBK|GB2312
	 * @mandatory: false 
	 * @default:UTF-8
	 */
	public final static String encoding = "encoding";
	/*
	 * @name: bufferSize
	 * @description: how large the buffer
	 * @range: [1024-4194304]
	 * @mandatory: false 
	 * @default: 4096
	 */
	public final static String bufferSize = "buffer_size";

	/*
   * @name: nullString
   * @description: specify nullString and replace it to null
   * @range: 
   * @mandatory: false
   * @default: \N
   */
	public final static String nullString = "nullstring";
	
	/*
	 * @name: colFilter
	 * @description:filter column
	 * @range: 
	 * @mandatory: false 
	 * @default: 
	 */		
	public final static String colFilter = "col_filter";

	 /*
	   * @name:concurrency
	   * @description:concurrency of the job 
	   * @range:1-30
	   * @mandatory: false
	   * @default:1
	   */
	public final static String concurrency = "concurrency";
	/*
	 * @name: firstLineReadSwitch
	 * @description: whether the first line to be read, if switch to false, the first line will be discarded
	 * @range: true,false
	 * @mandatory: false 
	 * @default: true 
	 */
	public final static String firstLineReadSwitch = "first_line_read_switch";
}
