package com.dp.nebula.wormhole.plugins.writer.mongowriter;

public final class ParamKey {
	/*
	 * @name: outputUri
	 * @description: mongo uri, format like: mongodb://[username:password@]host1[:port1][,host2[:port2],...[,hostN[:portN]]][/[database][?options]
	 * @range:
	 * @mandatory: true
	 * @default:mongodb://127.0.0.1:27017/db.coll
	 */
	public final static String outputUri = "output_uri";
	/*
	 * @name: outputFields
	 * @description: The fields, in JSON, to write, format like { _id:1, name:1, age:1, sex:1 } 
	 * @range:
	 * @mandatory: true
	 * @default:{ _id:1 }
	 */
	public final static String outputFields = "output_fields";
	/*
	 * @name: bulkInsertLine
	 * @description: bulk insertion line count
	 * @range:
	 * @mandatory: false
	 * @default:3000
	 */
	public final static String bulkInsertLine = "bulk_insert_line";
	 /*
	  * @name: concurrency
	  * @description: concurrency of the job 
	  * @range:1-10
	  * @mandatory: false
	  * @default:1
	  */
	public final static String concurrency = "concurrency";
	 /*
	  * @name: dropCollectionBeforeInsertionSwitch
	  * @description: whether to drop collection before insert data into collection
	  * @range: true, false
	  * @mandatory: false
	  * @default:false
	  */
	public final static String dropCollectionBeforeInsertionSwitch = "dropCollectionBeforeInsertionSwitch";
}
