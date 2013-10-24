package com.dp.nebula.wormhole.plugins.writer.greenplumwriter;

public final class ParamKey {

	/*
     * @name: connectProps
     * @description: id of Greenplum database's connect string properties
     * @range: if name is testProp, then you can set testProp.ip, testProp.port and so on in the WORMHOLE_CONNECT_FILE
     * @mandatory: false
     * @default:
     */
	public final static String connectProps = "connectProps";
	 /*
     * @name: ip
     * @description: Greenplum database's ip address
     * @range:
     * @mandatory: false
     * @default:
     */
	public final static String ip = "ip";
	/*
     * @name: port
     * @description: Greenplum database's port
     * @range:
     * @mandatory: false
     * @default:5432
     */
	public final static String port = "port";
	/*
     * @name: dbname
     * @description: Greenplum database's name
     * @range:
     * @mandatory: false
     * @default:
     */
	public final static String dbname = "dbname";
	/*
     * @name: username
     * @description: Greenplum database's login name
     * @range:
     * @mandatory: false
     * @default:
     */
	public final static String username = "username";
	/*
     * @name: password
     * @description: Greenplum database's login password
     * @range:
     * @mandatory: false
     * @default:
     */
	public final static String password = "password";
	
	/*
     * @name: priority
     * @description: priority of writer when error occurs. 0 indicates the highest priority.
     * 	   If error occurs in more than two writers, system will return error code of writer with the high priority.
     * @range: 0-99
     * @mandatory: false
     * @default: 0
     */
	public final static String priority = "priority";
	/*
     * @name: encoding
     * @description: 
     * @range: UTF-8|GBK|GB2312
     * @mandatory: false
     * @default: UTF-8
     */
	public final static String encoding = "encoding";
   /*
    * @name:params
    * @description:greenplum driver params
    * @range:params1|params2|...
    * @mandatory: false
    * @default:
    */
	public final static String greenplumParams = "params";
	/*
     * @name: tableName
     * @description: table name to copy into
     * @range: should be integral, like relation_name.table_name
     * @mandatory: true
     * @default: 
     */
	public final static String table = "tableName";
	/*
     * @name: columns
     * @description: columns to insert
     * @range: 
     * @mandatory: false
     * @default: 
     */
	public final static String columns = "columns";
	/*
	 * @name: pre
	 * @description: execute sql before dumping data
	 * @range:
	 * @mandatory: false
	 * @default:
	 */
	public final static String pre = "pre";
	/*
	 * @name: post
	 * @description: execute sql after dumping data
	 * @range:
	 * @mandatory: false
	 * @default:
	 */
	public final static String post = "post";
	/*
	   * @name: countSql
	   * @description: count the number of data lines inserted into db
	   * @range: 
	   * @mandatory: false
	   * @default: 
	   */
	public final static String countSql = "countSql";
	 /*
      * @name: rollback
      * @description: the roll back sql
      * @range: 
      * @mandatory: false
      * @default: 
      */
	public final static String rollback = "rollback";
	
	/*
     * @name: logErrorTable
     * @description: the error log well be insert into this table
     * @range: 
     * @mandatory: false
     * @default: 
     */
	public final static String logErrorTable = "logErrorTable";
	
	/*
     * @name: failedlinesthreshold
     * @description: the data volume read minus data volume wrote cannot larger than this number
     * @range: 0 or larger than or equals to 2
     * @mandatory: false
     * @default: 0
     */
	public final static String failedLinesThreshold = "failedlinesthreshold";
}

