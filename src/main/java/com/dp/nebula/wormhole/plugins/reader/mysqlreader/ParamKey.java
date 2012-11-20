package com.dp.nebula.wormhole.plugins.reader.mysqlreader;

public final class ParamKey {
		/*
		 * @name: connectProps
		 * @description: id of Mysql database's connect string properties
		 * @range: if name is testProp, then you can set testProp.ip, testProp.port and so on in the WORMHOLE_CONNECT_FILE
		 * @mandatory: false
		 * @default:
		 */
		public final static String connectProps = "connectProps";
		 /*
	       * @name: ip
	       * @description: Mysql database's ip address
	       * @range:
	       * @mandatory: false
	       * @default:
	       */
		public final static String ip = "ip";
		/*
	       * @name: port
	       * @description: Mysql database's port
	       * @range:
	       * @mandatory: false
	       * @default:3306
	       */
		public final static String port = "port";
		/*
	       * @name: dbname
	       * @description: Mysql database's name
	       * @range:
	       * @mandatory: false
	       * @default:
	       */
		public final static String dbname = "dbname";
		/*
	       * @name: username
	       * @description: Mysql database's login name
	       * @range:
	       * @mandatory: false
	       * @default:
	       */
		public final static String username = "username";
		/*
	       * @name: password
	       * @description: Mysql database's login password
	       * @range:
	       * @mandatory: false
	       * @default:
	       */
		public final static String password = "password";
		
		/*
	       * @name: encoding
	       * @description: mysql database's encode
	       * @range: UTF-8|GBK|GB2312
	       * @mandatory: false
	       * @default: UTF-8
	       */
		public final static String encoding = "encoding";
		
       /*
	       * @name: params
	       * @description: mysql driver params, starts with no &, e.g. loginTimeOut=3000&yearIsDateType=false
	       * @range: 
	       * @mandatory: false
	       * @default:
	       */
		public final static String mysqlParams = "params";
		
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
	       * @mandatory: false
	       * @default: 
	       */
		public final static String sql = "sql";
		/*
	     * @name: needSplit
	     * @description: if the sql need to be split
	     * @range: 
	     * @mandatory: true
	     * @default: true
	     */
		public final static String needSplit = "needSplit";
		 /*
	       * @name: concurrency
	       * @description: concurrency of the job
	       * @range: 1-10
	       * @mandatory: false
	       * @default: 1
	       */
		public final static String concurrency = "concurrency";
		
		 /*
	       * @name: blockSize
	       * @description: the block size in which the mysql data is read
	       * @range: 
	       * @mandatory: false
	       * @default: 1000
	       */
		public final static String blockSize = "blockSize";
		
		/*
	       * @name: tableName
	       * @description: table to export data
	       * @range: 
	       * @mandatory: false
	       * @default: 
	       */
		
		public final static String tableName = "tableName";
		
		 /*
	       * @name: autoIncKey
	       * @description: auto-increasing key of the table, use when split sql by MysqlReaderKeySplitter
	       * @range: 
	       * @mandatory: false
	       * @default: 
	       */
		public final static String autoIncKey = "autoIncKey";
		
		/*
	       * @name: columns
	       * @description: columns to be selected, default is *
	       * @range: 
	       * @mandatory: false
	       * @default: *
	       */
		public final static String columns = "columns";
		
		/*
	       * @name: where
	       * @description: where clause, like 'modified_time > sysdate'
	       * @range: eg start_time > '2012-04-01' and start_time < '2012-04-10'
	       * @mandatory: false
	       * @default: 
	       */
		public final static String where = "where";

		/*
	     * @name: countSql
	     * @description: if use complicated sql, you'd better fill this field
	     * @range: 
	     * @mandatory: false
	     * @default: 
	     */
		public final static String countSql = "countSql";
}
