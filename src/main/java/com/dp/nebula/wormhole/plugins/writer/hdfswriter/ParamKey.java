package com.dp.nebula.wormhole.plugins.writer.hdfswriter;

public final class ParamKey {
	/*
	 * @name: dir
	 * @description: hdfs dir，hdfs://ip:port/path
	 * @range:
	 * @mandatory: true 
	 * @default:
	 */
	public final static String dir = "dir";
	/*
	 * @name: prefixname
	 * @description: hdfs filename
	 * @range:
	 * @mandatory: false 
	 * @default: prefix
	 */
	public final static String prefixname = "prefix_filename";

	/*
	 * @name: fieldSplit
	 * @description: how to seperate fields
	 * @range:\t,\001,","
	 * @mandatory: false 
	 * @default:\t
	 */
	public final static String fieldSplit = "field_split";
	/*
	 * @name: lineSplit
	 * @description: how to seperate fields
	 * @range:\n
	 * @mandatory: false 
	 * @default:\n
	 */
	public final static String lineSplit = "line_split";
	/*
	 * @name: encoding 
	 * @description: encode
	 * @range: UTF-8|GBK|GB2312
	 * @mandatory: false
	 * @default: UTF-8
	 */
	public final static String encoding = "encoding";
	/*
	 * @name: nullChar
	 * @description: how to replace null in hdfs
	 * @range: 
	 * @mandatory: false
	 * @default:
	 */
	public final static String nullChar = "nullchar";
	/*
	 * @name: replaceChar
	 * @description: replace characters, if this parameter is not set, we will replace \r \n and splitField with ' ' as default
	 * @range: e.g:\r\n:\001 it means replace \r and \n with \001    
	 * @mandatory: false
	 * @default:
	 */
	public final static String replaceChar = "replace_char";
	/*
	 * @name: codecClass
	 * @description: compress codecs
	 * @range:com.hadoop.compression.lzo.LzopCodec|org.apache.hadoop.io.compress.BZip2Codec|org.apache.hadoop.io.compress.DefaultCodec|org.apache.hadoop.io.compress.GzipCodec
	 * @mandatory: false 
	 * @default: com.hadoop.compression.lzo.LzopCodec
	 */
	public final static String codecClass = "codec_class";
	/*
	 * @name: bufferSize
	 * @description: how much the buffer size is
	 * @range: [1024-4194304]
	 * @mandatory: false 
	 * @default: 4096
	 */
	public final static String bufferSize = "buffer_size";
	/*
	 * @name: fileType
	 * @description: TXT->TextFile,TXT_COMP->Compressed TextFile
	 * @range: TXT|TXT_COMP
	 * @mandatory: true 
	 * @default: TXT
	 */
	public final static String fileType = "file_type";
	/*
	 * @name:concurrency
	 * @description:concurrency of the job,,it also equals to split number
	 * @range:1-100
	 * @mandatory: false
	 * @default:1
	 */
	public final static String concurrency = "concurrency";
	/*
	 * @name: hiveTableAddPartitionSwitch
	 * @description: hiveTableAddPartitionSwitch switch 
	 * @range: true,false
	 * @mandatory: false
	 * @default: false
	 */
	public final static String hiveTableAddPartitionSwitch = "hive_table_add_partition_switch";
	/*
	 * @name: hiveTableAddPartitionSwitch
	 * @description: specify table and partition condition,this parameter is valid only if hiveTableAddPartitionSwitch is set to true
	 * @range: e.g:dt='2010-01-01'@sampleDatabase.sampleTable
	 * @mandatory: false
	 * @default:
	 */
	public final static String hiveTableAddPartitionCondition = "hive_table_add_partition_condition";

	/*
     * @name: dataTransformClass
     * @description: data transformer class path
     * @range: 
     * @mandatory: false
     * @default: 
     */
	public final static String dataTransformClass = "dataTransformClass";
	
	/*
     * @name: dataTransformClass
     * @description: data transformer paramas
     * @range: 
     * @mandatory: false
     * @default: 
     */
	public final static String dataTransformParams = "dataTransformParams";
	
	/*
     * @name: createLzoIndexFile
     * @description: whether to create lzo index file
     * @range: true,false
	 * @mandatory: false
	 * @default: true
     */
	public final static String createLzoIndexFile = "createLzoIndexFile";
}
