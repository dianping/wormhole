package com.dp.nebula.wormhole.plugins.reader.greenplumreader;

public class ParamKey {
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
     * @name: encoding
     * @description: Greenplum database's encode
     * @range: UTF-8|GBK|GB2312
     * @mandatory: false
     * @default: UTF-8
     */
	public final static String encoding = "encoding";
	
	/*
	 * @name:greenplum.params
	 * @description:greenplum driver params
	 * @range:params1|params2|...
	 * @mandatory: false
	 * @default:
	 */
	public final static String greenplumParams = "params";
	
	/*
     * @name: preCheck
     * @description: the front check sql, if this sql return 0 ,then the job will not go on and return an errorCode 4
     * @range: 
     * @mandatory: false
     * @default:
     */
	public final static String preCheck = "preCheck";
	
	/*
     * @name: sql
     * @description: self-defined sql statement
     * @range: 
     * @mandatory: true
     * @default: 
     */
	public final static String sql = "sql";
	
	/*
     * @name: tableName
     * @description: table to export data
     * @range: 
     * @mandatory: false
     * @default: 
     */
	
	public final static String tableName = "tableName";
	
	/*
     * @name: where
     * @description: where clause, like 'modified_time > sysdate'
     * @range: 
     * @mandatory: false
     * @default: 
     */
	public final static String where = "where";
	
	/*
     * @name: columns
     * @description: columns to be selected, default is *
     * @range: 
     * @mandatory: false
     * @default: *
     */
	public final static String columns = "columns";
	
	 /*
    * @name: concurrency
    * @description: concurrency of the job
    * @range: 1-10
    * @mandatory: false
    * @default: 1
    */
	public final static String concurrency = "concurrency";
	/*
     * @name: needSplit
     * @description: if the sql need to be split
     * @range: 
     * @mandatory: true
     * @default: true
     */
	public final static String needSplit = "needSplit";
	 /*
    * @name: partitionName
    * @description: split table by partition of greenplum table
    * @range: 
    * @mandatory: false
    * @default: 
    */
	public final static String partitionName = "partitionName";
	
	 /*
    * @name: partitionValue
    * @description: split table by partition of greenplum table
    * @range: eg: 2012-03-01~2012-03-17 2012-03-19
    * @mandatory: false
    * @default: 
    */
	public final static String partitionValue = "partitionValue";
	
	/*
   * @name: countSql
   * @description: count the number of data lines selected from db. if use complicated sql, you'd better fill this field
   * @range: 
   * @mandatory: false
   * @default: 
   */
	public final static String countSql = "countSql";

}
