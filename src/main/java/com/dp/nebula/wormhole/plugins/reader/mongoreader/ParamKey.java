package com.dp.nebula.wormhole.plugins.reader.mongoreader;

public final class ParamKey {
	/*
	 * @name: inputUri
	 * @description: mongo uri, format like: mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]
	 * @range:
	 * @mandatory: true
	 * @default:mongodb://127.0.0.1:27017/db.coll
	 */
	public final static String inputUri = "input_uri";
	/*
	 * @name: inputFields
	 * @description: The fields, in JSON, to read 
	 * @range:
	 * @mandatory: true
	 * @default:{ _id:1 }
	 */
	public final static String inputFields = "input_fields";
	/*
	 * @name: inputQuery
	 * @description: The query, in JSON, to execute [OPTIONAL]
	 * @range:
	 * @mandatory: false
	 * @default:
	 */
	public final static String inputQuery = "input_query";
	/*
	 * @name: inputSort
	 * @description: A JSON sort specification for read [OPTIONAL], opposite operation due to the storage implementation
	 * @range:
	 * @mandatory: false
	 * @default:
	 */
	public final static String inputSort = "input_sort";
	/*
	 * @name: inputLimit
	 * @description: The number of documents to limit to for read
	 * @range:
	 * @mandatory: false
	 * @default:
	 */
	public final static String inputLimit = "input_limit";
	/*
	 * @name:needSplit
	 * @description: split switch 
	 * @range:true,false
	 * @mandatory:true
	 * @default:false
	 */
	public final static String needSplit = "need_split";
	/*
	 * @name:splitKeyPattern
	 * @description: split key JSON pattern, it must be an index on the collection
	 * @range:
	 * @mandatory: false
	 * @default:{ "_id": 1 }
	 */
	public final static String splitKeyPattern = "split_key_pattern";
	/*
	 * @name:splitSize
	 * @description: if you want to control the split size for input, set it here, it should be an integer which refer to megabytes 
	 * @range:1-256
	 * @mandatory: false
	 * @default:8
	 */
	public final static String splitSize = "split_size";
	 /*
     * @name: concurrency
     * @description: concurrency of the job
     * @range: 1-10
     * @mandatory: false
     * @default: 1
     */
	public final static String concurrency = "concurrency";
	/*
     * @name: field_need_split
     * @description: In some mongo tables, all fields are integrated into one field. 
     * 	This field indicates whether these fields should be split from the first field
     * @range: true,false
     * @mandatory: false
     * @default: 
     */
	public final static String fieldNeedSplit = "field_need_split";
	/*
     * @name: field_split_char
     * @description: In some mongo tables, all fields are integrated into one field. 
     * 	Plugin can split these fields use this character as splitter.
     * @range: eg. \t
     * @mandatory: false
     * @default: 
     */
	public final static String filedSplitChar = "field_split_char";
	/*
     * @name: dataTransformClass
     * @description: data transformer class path
     * @range: 
     * @mandatory: false
     * @default: 
     */
	public final static String dataTransformClass = "dataTransformClass";

}
