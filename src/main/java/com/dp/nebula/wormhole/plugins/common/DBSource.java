package com.dp.nebula.wormhole.plugins.common;

//import com.dp.nebula.wormhole.common.WormholeException;
import com.dp.nebula.wormhole.common.interfaces.IPlugin;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.BasicDataSourceFactory;
import org.apache.log4j.Logger;

import javax.sql.DataSource;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * A tool class which wraps DataSource. It provides methods to create a
 * DataSource, get data source, get database connection so that other plugins
 * can use these methods to work easily.<br>
 * This class is mainly designed to avoid problem while occurs while data source
 * and destination is the same kind of DataBase.
 * 
 * for usage, see {@link MysqlReader}
 * 
 */
public final class DBSource {
	private static Logger logger = Logger.getLogger(DBSource.class);

	public static final Map<String, DataSource> sourceInfoMap = new HashMap<String, DataSource>();

	private DBSource() {
	}

	/**
	 * A synchronized static method which create {@link DataSource}.
	 * 
	 * @param clazz
	 *            {@link Pluginable} class
	 * 
	 * @param ip
	 *            database host ip address
	 * 
	 * @param dbname
        *            database name
        *
        * @param port
	 *            database port
	 * 
	 * @param p
	 *            Properties file.
	 * 
	 * @return true if create DataSource successfully, false if failed.
	 * @throws Exception 
	 * 
	 * @throws IllegalStateException
	 * 
	 */
	public static boolean register(Class<? extends Object> clazz, String ip,
			String port, String dbname, Properties p) throws Exception {
		String id = genKey(clazz, ip, port, dbname);
		return register(id, p);
	}

	/**
	 * A synchronized static method which create {@link DataSource}. NOTE:
	 * Client must make sure all connections to the same database should share
	 * the same value a suggestion: use genKey we provide below
	 * 
	 * @param key
	 *            key to query database source
	 * 
	 * @param p
	 *            Properties file.
	 * 
	 * @return true if create DataSource successfully, false if failed.
	 * 
	 * @throws IllegalStateException
	 * 
	 */
	public static synchronized boolean register(String key, Properties p) throws Exception{
		boolean succeed = false;

		if (!sourceInfoMap.containsKey(key)) {
			BasicDataSource dataSource = null;
			try {
				dataSource = (BasicDataSource) BasicDataSourceFactory
						.createDataSource(p);
			} catch (Exception e) {
				logger.error(String.format(
						"Key [%s] register database pool failed .", key));
				throw e;
			}
			if (null != dataSource) {
				dataSource.setAccessToUnderlyingConnectionAllowed(true);
				sourceInfoMap.put(key, dataSource);
				logger.info(String.format(
						"Key [%s] register database pool successfully .", key));
				succeed = true;
			} else {
				logger.error(String.format(
						"Key [%s] register database pool failed .", key));
				throw new Exception("register database pool failed .");
			}
		} else {
			logger.error(String.format("Key [%s] already in database pool .",
					key));
		}
		return succeed;
	}

	/**
	 * A synchronized static method which gets DataSoucre by the identification.
	 * 
	 * @param clazz
	 *            {@link Pluginable} class
	 * 
	 * @param ip
	 *            database host ip address
	 * 
	 * @param port
	 *            database port
	 * 
	 * @param dbname
	 *            database name
	 * 
	 * @param p
	 *            Properties file.
	 * 
	 * 
	 * @return a binded DataSource.
	 * @throws Exception 
	 * 
	 */
	public static DataSource getDataSource(Class<? extends IPlugin> clazz,
			String ip, String port, String dbname) throws Exception {
		return getDataSource(genKey(clazz, ip, port, dbname));
	}

	/**
	 * A synchronized static method which gets DataSoucre by the identification.
	 * 
	 * @param key
	 *            unique identification binding with DataSource.
	 * 
	 * @return a binded DataSource.
	 * @throws Exception 
	 * 
	 */
	public static synchronized DataSource getDataSource(String key) throws Exception {
		DataSource source = sourceInfoMap.get(key);
		if (null == source) {
			throw new Exception(String.format(
					"Cannot get DataSource specified by key [%s] .",key));
		}
		return source;
	}

	/**
	 * A synchronized static method which gets database connection by the
	 * identification.
	 * 
	 * @param clazz
	 *            {@link Pluginable} class
	 * 
	 * @param ip
	 *            database host ip address
	 * 
	 * @param port
	 *            database port
	 * 
	 * @param dbname
	 *            database name
	 * 
	 * @return a database connection.
	 * @throws Exception 
	 * 
	 * @throws {@link IllegalStateException} Cannot connect to DataBase
	 * 
	 *         {@link IllegalArgumentException} Connect id is not registered
	 * 
	 */
	
	public static Connection getConnection(Class<? extends Object> clazz,
			String ip, String port, String dbname) throws Exception {
		return getConnection(genKey(clazz, ip, port, dbname),port);
	}

	/**
	 * A synchronized static method which gets database connection by the
	 * identification.
	 * 
	 * @param id
	 *            unique identification binding with DataSource.
	 * 
	 * @return a database connection.
	 * @throws Exception 
	 * 
	 * @throws {@link IllegalStateException} Cannot connect to DataBase
	 * 
	 *               {@link IllegalArgumentException} Connect id is not registered
	 * 
	 */
	public static synchronized Connection getConnection(String id,String pluginID) throws Exception {
		Connection c = null;
		BasicDataSource dataSource = (BasicDataSource) sourceInfoMap.get(id);
		try {
			c = dataSource.getConnection();
		} catch (SQLException e) {
			logger.error(e);
			throw e;
		}
		if (null != c) {
			logger.debug(String.format(
					pluginID + "-Key [%s] connect to database pool successfully .", id));
		} else {
			logger.error(String.format(
					pluginID + "-Key [%s]  connect to database pool failed .", id));
			throw new Exception("Connection key [%s] error .");
		}
		return c;
	}

	/**
	 * generate key to get {@link DataSource}
	 * 
	 * NOTE: Client must make sure all connections to the same database should
	 * share the same value a suggestion: use genKey we provide below, we used
	 * MD5 encryption method.
	 * 
	 *
        * @param clazz             Class for Plugin
        *
        *
        * @param ip                   database ip
        *
        * @param port               database port
        *
        * @param dbname         database name
        *
        *  @return                      an unique key
     * */
	public static String genKey(Class<? extends Object> clazz, String ip,
			String port, String dbname) {
		String str = clazz.getCanonicalName() + "_" + ip + "_" + port + "_"
				+ dbname;
		return md5(str);
	}

	private static String md5(String key) {
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(key.getBytes());
			byte b[] = md.digest();
			int i;
			StringBuffer buf = new StringBuffer(32);
            for (byte aB : b) {
                i = aB;
                if (i < 0) {
                    i += 256;
                }
                if (i < 16) {
                    buf.append("0");
                }
                buf.append(Integer.toHexString(i));
            }
			return buf.toString().substring(8, 24);
		} catch (NoSuchAlgorithmException e) {
			logger.error(e.getMessage());
			return key;
		}
	}
}
