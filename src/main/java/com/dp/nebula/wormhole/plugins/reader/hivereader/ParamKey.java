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
     * @mandatory: false
     * @default: 
     */
	public final static String sql = "sql";
}
