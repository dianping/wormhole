package com.dp.nebula.wormhole.common.utils;

public abstract class Environment {
	public static final String USER_DIR = System.getProperty("user.dir");
	
	public static final String ENGINE_CONF = String.format("%s/conf/engine.xml", USER_DIR);
	public static final String PLUGINS_CONF = String.format("%s/conf/plugins.xml", USER_DIR);
	public static final String LOG4J_CONF = String.format("%s/conf/log4j.properties", USER_DIR);
	public static final String JOB_INFO_DB_PROP = String.format("%s/conf/jobInfoDB.properties", USER_DIR);
	public static final String READER_PLUGINS_DIR = String.format("%s/plugins/reader", USER_DIR);
	public static final String WRITER_PLUGINS_DIR = String.format("%s/plugins/writer", USER_DIR);

}