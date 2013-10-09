package com.dp.nebula.wormhole.plugins.reader.sftpreader;

public final class ParamKey {
	/*
	 * @name: dir
	 * @description: sftp path format like: sftp://[<user>@]<host>[:<port>]/<path>/<file>
	 * @range:
	 * @mandatory: true 
	 * @default:
	 */
	public final static String dir = "dir";
	/*
	 * @name: password
	 * @description: password
	 * @range:
	 * @mandatory: true 
	 * @default:
	 */
	public final static String password = "password";
	/*
	 * @name: fileType
	 * @description: fileType
	 * @range:txt,gz,lzo
	 * @mandatory: true
	 * @default:txt
	 */
	public final static String fileType = "file_type";
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
	 * @description: filter column
	 * @range: 
	 * @mandatory: false 
	 * @default: 
	 */		
	public final static String colFilter = "col_filter";
	/*
	 * @name: concurrency
	 * @description: concurrency of the job 
	 * @range:1-30
	 * @mandatory: false
	 * @default:1
	*/
	public final static String concurrency = "concurrency";
	/*
	 * @name: firstLineReadOrNot
	 * @description: whether the first line to be read, if set to false, the first line will be discarded
	 * @range: true,false
	 * @mandatory: false
	 * @default: true 
	 */
	public final static String firstLineReadOrNot = "first_line_read_or_not";

}
