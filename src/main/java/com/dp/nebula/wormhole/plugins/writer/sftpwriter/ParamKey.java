package com.dp.nebula.wormhole.plugins.writer.sftpwriter;

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
	 * @name: prefixname
	 * @description: prefix filename
	 * @range:
	 * @mandatory: false 
	 * @default:part
	 */
	public final static String prefixname = "prefix_filename";
	/*
	 * @name: fileType
	 * @description: fileType
	 * @range: txt|gz|lzo
	 * @mandatory: true
	 * @default: txt
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
	 * @name: lineSplit
	 * @description: how to seperate fields
	 * @range:\n
	 * @mandatory: false 
	 * @default:\n
	 */
	public final static String lineSplit = "line_split";
	/*
	 * @name: encoding 
	 * @description: file encode
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
	 * @name: nullChar
	 * @description: how to replace null in remote server
	 * @range: 
	 * @mandatory: false
	 * @default:
	 */
	public final static String nullChar = "nullchar";
	/*
	 * @name: concurrency
	 * @description: concurrency of the job 
	 * @range:1-30
	 * @mandatory: false
	 * @default:1
	*/
	public final static String concurrency = "concurrency";
}