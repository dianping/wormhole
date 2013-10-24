package com.dp.nebula.wormhole.plugins.reader.sqlserverreader;

public final class ParamKey {
		/*
		 * @name: connectProps
		 * @description: id of SqlServer database's connect string properties
		 * @range: if name is testProp, then you can set testProp.ip, testProp.port and so on in the WORMHOLE_CONNECT_FILE
		 * @mandatory: false
		 * @default:
		 */
		public final static String connectProps = "connectProps";
		 /*
	       * @name: ip
	       * @description: SqlServer database's ip address
	       * @range:
	       * @mandatory: false
	       * @default:
	       */
		public final static String ip = "ip";
		/*
	       * @name: port
	       * @description: SqlServer database's port
	       * @range:
	       * @mandatory: false
	       * @default:1433
	       */
		public final static String port = "port";
		/*
	       * @name: dbname
	       * @description: SqlServer database's name
	       * @range:
	       * @mandatory: false
	       * @default:
	       */
		public final static String dbname = "dbname";
		/*
	       * @name: username
	       * @description: SqlServer database's login name
	       * @range:
	       * @mandatory: false
	       * @default:
	       */
		public final static String username = "username";
		/*
	       * @name: password
	       * @description: SqlServer database's login password
	       * @range:
	       * @mandatory: false
	       * @default:
	       */
		public final static String password = "password";
		
		public final static String url = "url";
		
		/*
	       * @name: encoding
	       * @description: SqlServer database's encode
	       * @range: UTF-8|GBK|GB2312
	       * @mandatory: false
	       * @default: UTF-8
	       */
		public final static String encoding = "encoding";
		
       /*
	       * @name: SqlServer.params
	       * @description: SqlServer driver params
	       * @range: 
	       * @mandatory: false
	       * @default:
	       */
		public final static String sqlServerParams = "params";
		
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
	       * @description: the block size in which the SqlServer data is read
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
	       * @description: auto-increasing key of the table, use when split sql by SqlServerReaderKeySplitter
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
