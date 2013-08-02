package com.dp.nebula.wormhole.plugins.writer.mysqlwriter;

public final  class ParamKey {
	
	private ParamKey(){
	}
	/*
     * @name: connectProps
     * @description: id of Mysql database's connect string properties
     * @range: if name is testProp, then you can set testProp.ip, testProp.port and so on in the WORMHOLE_CONNECT_FILE
     * @mandatory: false
     * @default:
     */
	public static final String CONNECT_PROPS = "connectProps";
	 /*
      * @name: ip
      * @description: Mysql database ip address
      * @range:
      * @mandatory: false
      * @default:
      */
	public static final String IP = "ip";
	/*
      * @name: port
      * @description: Mysql database port
      * @range:
      * @mandatory: false
      * @default:3306
      */
	public static final String PORT = "port";
	/*
      * @name: dbname
      * @description: Mysql database name
      * @range:
      * @mandatory: false
      * @default:
      */
	public static final String DBNAME = "dbname";
	/*
      * @name: username
      * @description: Mysql database login username
      * @range:
      * @mandatory: false
      * @default:
      */
	public static final String USER_NAME = "username";
	/*
      * @name: password
      * @description: Mysql database login password
      * @range:
      * @mandatory: false
      * @default:
      */
	public static final String PASSWORD = "password";
	/*
     * @name: priority
     * @description: priority of writer when error occurs. 0 indicates the highest priority.
     * 	   If error occurs in more than two writers, system will return error code of writer with the high priority.
     * @range: 0-99
     * @mandatory: false
     * @default: 0
     */
	public static final String PRIORITY = "priority";
	/*
     * @name: encoding
     * @description: 
     * @range: UTF-8|GBK|GB2312
     * @mandatory: false
     * @default: UTF-8
     */
	public static final String ENCODING = "encoding";
	
    /*
     * @name:params
     * @description:mysql driver params
     * @range:params1|params2|...
     * @mandatory: false
     * @default:
     */
	public static final String MYSQL_PRAMAS = "params";
	/*
     * @name: loadFile
     * @description: whether write data to mysql on way of load file.
     * @range: true|false
     * @mandatory: true
     * @default: false
     */
	public static final String LOADFILE = "loadFile";
	/*
      * @name: tableName
      * @description: table to be dumped data into
      * @range: 
      * @mandatory: true
      * @default: 
      */
	public static final String TABLE = "tableName";
	/*
      * @name: columns
      * @description: columns need to insert
      * @range: 
      * @mandatory: false
      * @default:
      */
	public static final String COLUMNS = "columns";
	/*
	 * @name: pre
	 * @description: execute sql before dumping data
	 * @range:
	 * @mandatory: false
	 * @default:
	 */
	public static final String PRE = "pre";
	/*
	 * @name: post
	 * @description: execute sql after dumping data
	 * @range:
	 * @mandatory: false
	 * @default:
	 */
	public static final String POST = "post";
	/*
	 * @name: OPERATION
	 * @description: which operation to perform
	 * @range: INSERT|REPLACE|UPDATE
	 * @mandatory: false
	 * @default:INSERT
	 */
	public static final String OPERATION = "OPERATION";

	 /*
      * @name: rollback
      * @description: the roll back sql
      * @range: 
      * @mandatory: false
      * @default: 
      */
	public static final String ROLL_BACK = "rollback";
	
	/*
     * @name: failedlinesthreshold
     * @description: the data volume read minus data volume wrote cannot larger than this number
     * @range: 
     * @mandatory: false
     * @default: 0
     */
	public static final String FAIL_LINES = "failedlinesthreshold";
	
	/*
	   * @name: countSql
	   * @description: count the number of data lines inserted into db
	   * @range: 
	   * @mandatory: false
	   * @default: 
	   */
	public static final String COUNT_SQL = "countSql";
}

