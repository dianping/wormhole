package com.dp.nebula.wormhole.plugins.reader.hivereader;

public final class ParamKey {
	/*
	 * @name:path
     * @description:hive path ,format like "jdbc:hive://192.168.7.80:10000/default"
     * @range:
	 * @mandatory:true
	 * @default:jdbc:hive://10.1.1.161:10000/default
	 */
	public final static String path = "path";
	/*
     * @name: username
     * @description: hive login name
     * @range:
     * @mandatory: false
     * @default:
     */
	public final static String username = "username";
	/*
     * @name: password
     * @description: hive login password
     * @range:
     * @mandatory: false
     * @default:
     */
	public final static String password = "password";
	/*
     * @name: sql
     * @description: self-defined sql statement
     * @range: 
     * @mandatory: true
     * @default: 
     */
	public final static String sql = "sql";
	/*
     * @name: mode
     * @description: query mode, READ_FROM_HIVESERVER: fetch data directly from hive server, READ_FROM_HDFS: insert the data into hdfs directory and fetch data directly from datanode 
     * @range: READ_FROM_HIVESERVER,READ_FROM_HDFS
     * @mandatory: true
     * @default: READ_FROM_HIVESERVER
     */
	public final static String mode = "mode";
	/*
     * @name: dataDir
     * @description: the temporary data directory to fetch on hdfs if using mode 2 
     * @range:
     * @mandatory: true
     * @default:hdfs://10.2.6.102/tmp/
     */
	public final static String dataDir = "dataDir";
	 /*
	  * @name:reduceNumber
	  * @description:reduce task number when doing insert query, when it set to -1, hive will automatically computer reduce number 
	  * @range:1-1000
	  * @mandatory: true
	  * @default:-1
	  */
	public final static String reduceNumber = "reduceNumber";
	 /*
	  * @name:concurrency
	  * @description:concurrency of the job 
	  * @range:1-10
	  * @mandatory: false
	  * @default:1
	  */
	public final static String concurrency = "concurrency";
	
}
